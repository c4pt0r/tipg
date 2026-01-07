use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use sqlparser::ast::{Assignment, Expr, Ident, OnConflictAction, OnInsert, SelectItem};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tikv_client::Transaction;

use super::expr::eval_expr;
use super::helpers::{coerce_value_for_column, eval_default_expr};
use crate::storage::TikvStore;
use crate::types::{ColumnDef, Row, TableSchema, Value};

pub fn build_returning_columns(
    returning: &Option<Vec<SelectItem>>,
    schema: &TableSchema,
) -> Result<Vec<String>> {
    let mut ret_cols = Vec::new();
    if let Some(items) = returning {
        for item in items {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(id)) => ret_cols.push(id.value.clone()),
                SelectItem::ExprWithAlias { alias, .. } => ret_cols.push(alias.value.clone()),
                SelectItem::Wildcard(_) => {
                    for c in &schema.columns {
                        ret_cols.push(c.name.clone());
                    }
                }
                _ => return Err(anyhow!("Unsupported RETURNING")),
            }
        }
    }
    Ok(ret_cols)
}

pub fn eval_returning_row(
    returning: &Option<Vec<SelectItem>>,
    row: &Row,
    schema: &TableSchema,
) -> Result<Option<Row>> {
    if let Some(items) = returning {
        let mut vals = Vec::new();
        for item in items {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    vals.push(eval_expr(e, Some(row), Some(schema))?);
                }
                SelectItem::Wildcard(_) => vals.extend(row.values.clone()),
                _ => {}
            }
        }
        Ok(Some(Row::new(vals)))
    } else {
        Ok(None)
    }
}

pub async fn execute_insert_row(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    schema: &TableSchema,
    row: Row,
    on_conflict: &Option<OnInsert>,
) -> Result<Option<Row>> {
    let pk_values = schema.get_pk_values(&row);

    if !schema.foreign_keys.is_empty() {
        validate_foreign_keys(store, txn, schema, &row).await?;
    }

    let insert_result = store.insert(txn, table_name, row.clone()).await;
    match insert_result {
        Ok(()) => {
            for index in &schema.indexes {
                let idx_values = schema.get_index_values(index, &row);
                let result = store
                    .create_index_entry(
                        txn,
                        schema.table_id,
                        index.id,
                        &idx_values,
                        &pk_values,
                        index.unique,
                    )
                    .await;
                if let Err(e) = result {
                    if e.to_string().contains("Duplicate entry") {
                        let cols = index.columns.join(", ");
                        let vals: Vec<String> =
                            idx_values.iter().map(|v| format!("{}", v)).collect();
                        return Err(anyhow!(
                            "duplicate key value violates unique constraint \"{}\"\nDETAIL:  Key ({})=({}) already exists.",
                            index.name,
                            cols,
                            vals.join(", ")
                        ));
                    }
                    return Err(e);
                }
            }
            Ok(Some(row))
        }
        Err(e) if e.to_string().contains("Duplicate primary key") => match on_conflict {
            Some(OnInsert::OnConflict(oc)) => match &oc.action {
                OnConflictAction::DoNothing => Ok(None),
                OnConflictAction::DoUpdate(do_update) => {
                    let existing_rows = store
                        .batch_get_rows(txn, schema.table_id, vec![pk_values.clone()], schema)
                        .await?;
                    if existing_rows.is_empty() {
                        return Err(anyhow!("Failed to fetch existing row for upsert"));
                    }
                    let existing_row = &existing_rows[0];
                    let mut updated_vals = existing_row.values.clone();
                    for assignment in &do_update.assignments {
                        let col_name = assignment.id.last().unwrap().value.clone();
                        let col_idx = schema
                            .column_index(&col_name)
                            .ok_or_else(|| anyhow!("Unknown column in DO UPDATE: {}", col_name))?;
                        updated_vals[col_idx] =
                            eval_expr(&assignment.value, Some(existing_row), Some(schema))?;
                    }
                    let updated_row = Row::new(updated_vals);
                    update_row_indexes(store, txn, schema, existing_row, &updated_row).await?;
                    store.upsert(txn, table_name, updated_row.clone()).await?;
                    Ok(Some(updated_row))
                }
            },
            Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
                let existing_rows = store
                    .batch_get_rows(txn, schema.table_id, vec![pk_values.clone()], schema)
                    .await?;
                if existing_rows.is_empty() {
                    return Err(anyhow!("Failed to fetch existing row for upsert"));
                }
                let existing_row = &existing_rows[0];
                let mut updated_vals = existing_row.values.clone();
                for assignment in assignments {
                    let col_name = assignment.id.last().unwrap().value.clone();
                    let col_idx = schema
                        .column_index(&col_name)
                        .ok_or_else(|| anyhow!("Unknown column: {}", col_name))?;
                    updated_vals[col_idx] =
                        eval_expr(&assignment.value, Some(existing_row), Some(schema))?;
                }
                let updated_row = Row::new(updated_vals);
                update_row_indexes(store, txn, schema, existing_row, &updated_row).await?;
                store.upsert(txn, table_name, updated_row.clone()).await?;
                Ok(Some(updated_row))
            }
            None => Err(e),
            _ => Err(e),
        },
        Err(e) => Err(e),
    }
}

async fn update_row_indexes(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    schema: &TableSchema,
    old_row: &Row,
    new_row: &Row,
) -> Result<()> {
    let pk_values = schema.get_pk_values(old_row);
    for index in &schema.indexes {
        let old_idx = schema.get_index_values(index, old_row);
        store
            .delete_index_entry(
                txn,
                schema.table_id,
                index.id,
                &old_idx,
                &pk_values,
                index.unique,
            )
            .await?;
    }
    for index in &schema.indexes {
        let new_idx = schema.get_index_values(index, new_row);
        store
            .create_index_entry(
                txn,
                schema.table_id,
                index.id,
                &new_idx,
                &pk_values,
                index.unique,
            )
            .await?;
    }
    Ok(())
}

pub async fn handle_foreign_key_on_delete(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    schema: &TableSchema,
    row: &Row,
) -> Result<()> {
    let pk_values = schema.get_pk_values(row);

    let table_names = store.list_tables(txn).await?;
    let mut table_rows: HashMap<String, Vec<Row>> = HashMap::new();
    let mut table_schemas: HashMap<String, TableSchema> = HashMap::new();
    for t in &table_names {
        if let Some(s) = store.get_schema(txn, t).await? {
            if !s.foreign_keys.is_empty() {
                let rows = store.scan(txn, t).await?;
                table_rows.insert(t.clone(), rows);
                table_schemas.insert(t.clone(), s);
            }
        }
    }

    let mut deleted_pks: HashMap<String, Vec<Vec<Value>>> = HashMap::new();

    Box::pin(cascade_delete_recursive(
        store,
        txn,
        table_name,
        &pk_values,
        &table_rows,
        &table_schemas,
        &mut deleted_pks,
    ))
    .await
}

async fn cascade_delete_recursive(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    pk_values: &[Value],
    table_rows: &HashMap<String, Vec<Row>>,
    table_schemas: &HashMap<String, TableSchema>,
    deleted_pks: &mut HashMap<String, Vec<Vec<Value>>>,
) -> Result<()> {
    use crate::types::ForeignKeyAction;

    for (other_table, other_schema) in table_schemas {
        if other_table == table_name {
            continue;
        }

        for fk in &other_schema.foreign_keys {
            if fk.ref_table != table_name {
                continue;
            }

            let all_rows = table_rows.get(other_table).cloned().unwrap_or_default();
            let deleted_in_table = deleted_pks.entry(other_table.clone()).or_default();

            let mut rows_to_cascade: Vec<Row> = Vec::new();
            let mut rows_to_update: Vec<(Row, Row)> = Vec::new();

            for other_row in &all_rows {
                let other_pk = other_schema.get_pk_values(other_row);
                if deleted_in_table.contains(&other_pk) {
                    continue;
                }

                let mut fk_values: Vec<Value> = Vec::new();
                let mut all_null = true;

                for col_name in &fk.columns {
                    if let Some(idx) = other_schema.column_index(col_name) {
                        let val = other_row.values[idx].clone();
                        if val != Value::Null {
                            all_null = false;
                        }
                        fk_values.push(val);
                    }
                }

                if all_null {
                    continue;
                }

                if fk_values == pk_values {
                    match fk.on_delete {
                        ForeignKeyAction::Cascade => {
                            rows_to_cascade.push(other_row.clone());
                        }
                        ForeignKeyAction::SetNull => {
                            let mut new_values = other_row.values.clone();
                            for col_name in &fk.columns {
                                if let Some(idx) = other_schema.column_index(col_name) {
                                    new_values[idx] = Value::Null;
                                }
                            }
                            rows_to_update.push((other_row.clone(), Row::new(new_values)));
                        }
                        ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {
                            let cols = fk.ref_columns.join(", ");
                            let pk_val_strs: Vec<String> =
                                pk_values.iter().map(|v| format!("{}", v)).collect();
                            return Err(anyhow!(
                                "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"\n\
                                 DETAIL:  Key ({})=({}) is still referenced from table \"{}\".",
                                table_name,
                                fk.name,
                                other_table,
                                cols,
                                pk_val_strs.join(", "),
                                other_table
                            ));
                        }
                        ForeignKeyAction::SetDefault => {
                            let mut new_values = other_row.values.clone();
                            for col_name in &fk.columns {
                                if let Some(idx) = other_schema.column_index(col_name) {
                                    let col = &other_schema.columns[idx];
                                    let default_val = if let Some(ref def_expr) = col.default_expr {
                                        eval_default_expr(def_expr)?
                                    } else {
                                        Value::Null
                                    };
                                    new_values[idx] = default_val;
                                }
                            }
                            rows_to_update.push((other_row.clone(), Row::new(new_values)));
                        }
                    }
                }
            }

            for del_row in rows_to_cascade {
                let del_pk = other_schema.get_pk_values(&del_row);

                Box::pin(cascade_delete_recursive(
                    store,
                    txn,
                    other_table,
                    &del_pk,
                    table_rows,
                    table_schemas,
                    deleted_pks,
                ))
                .await?;

                deleted_pks
                    .entry(other_table.clone())
                    .or_default()
                    .push(del_pk.clone());

                store.delete_by_pk(txn, other_table, &del_pk).await?;
            }

            for (old_row, new_row) in rows_to_update {
                execute_update_row(store, txn, other_table, other_schema, &old_row, new_row)
                    .await?;
            }
        }
    }
    Ok(())
}

pub async fn handle_foreign_key_on_update(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    schema: &TableSchema,
    old_row: &Row,
    new_row: &Row,
) -> Result<()> {
    use crate::types::ForeignKeyAction;

    let old_pk_values = schema.get_pk_values(old_row);
    let new_pk_values = schema.get_pk_values(new_row);

    if old_pk_values == new_pk_values {
        return Ok(());
    }

    let table_names = store.list_tables(txn).await?;

    for other_table in &table_names {
        if other_table == table_name {
            continue;
        }

        let other_schema = match store.get_schema(txn, other_table).await? {
            Some(s) => s,
            None => continue,
        };

        for fk in &other_schema.foreign_keys {
            if fk.ref_table != table_name {
                continue;
            }

            let all_rows = store.scan(txn, other_table).await?;
            let mut rows_to_update: Vec<(Row, Row)> = Vec::new();

            for other_row in &all_rows {
                let mut fk_values: Vec<Value> = Vec::new();
                let mut all_null = true;

                for col_name in &fk.columns {
                    if let Some(idx) = other_schema.column_index(col_name) {
                        let val = other_row.values[idx].clone();
                        if val != Value::Null {
                            all_null = false;
                        }
                        fk_values.push(val);
                    }
                }

                if all_null {
                    continue;
                }

                if fk_values == old_pk_values {
                    match fk.on_update {
                        ForeignKeyAction::Cascade => {
                            let mut new_values = other_row.values.clone();
                            for (i, col_name) in fk.columns.iter().enumerate() {
                                if let Some(idx) = other_schema.column_index(col_name) {
                                    if i < new_pk_values.len() {
                                        new_values[idx] = new_pk_values[i].clone();
                                    }
                                }
                            }
                            rows_to_update.push((other_row.clone(), Row::new(new_values)));
                        }
                        ForeignKeyAction::SetNull => {
                            let mut new_values = other_row.values.clone();
                            for col_name in &fk.columns {
                                if let Some(idx) = other_schema.column_index(col_name) {
                                    new_values[idx] = Value::Null;
                                }
                            }
                            rows_to_update.push((other_row.clone(), Row::new(new_values)));
                        }
                        ForeignKeyAction::SetDefault => {
                            let mut new_values = other_row.values.clone();
                            for col_name in &fk.columns {
                                if let Some(idx) = other_schema.column_index(col_name) {
                                    let col = &other_schema.columns[idx];
                                    let default_val = if let Some(ref def_expr) = col.default_expr {
                                        eval_default_expr(def_expr)?
                                    } else {
                                        Value::Null
                                    };
                                    new_values[idx] = default_val;
                                }
                            }
                            rows_to_update.push((other_row.clone(), Row::new(new_values)));
                        }
                        ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {
                            let cols = fk.ref_columns.join(", ");
                            let pk_val_strs: Vec<String> =
                                old_pk_values.iter().map(|v| format!("{}", v)).collect();
                            return Err(anyhow!(
                                "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"\n\
                                 DETAIL:  Key ({})=({}) is still referenced from table \"{}\".",
                                table_name,
                                fk.name,
                                other_table,
                                cols,
                                pk_val_strs.join(", "),
                                other_table
                            ));
                        }
                    }
                }
            }

            for (old_child_row, new_child_row) in rows_to_update {
                Box::pin(execute_update_row(
                    store,
                    txn,
                    other_table,
                    &other_schema,
                    &old_child_row,
                    new_child_row,
                ))
                .await?;
            }
        }
    }
    Ok(())
}

pub async fn execute_delete_row(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    schema: &TableSchema,
    row: &Row,
) -> Result<()> {
    handle_foreign_key_on_delete(store, txn, table_name, schema, row).await?;

    let pks = schema.get_pk_values(row);
    store.delete_by_pk(txn, table_name, &pks).await?;
    for index in &schema.indexes {
        let idx_values = schema.get_index_values(index, row);
        store
            .delete_index_entry(
                txn,
                schema.table_id,
                index.id,
                &idx_values,
                &pks,
                index.unique,
            )
            .await?;
    }
    Ok(())
}

pub async fn execute_update_row(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    schema: &TableSchema,
    old_row: &Row,
    new_row: Row,
) -> Result<Row> {
    if !schema.foreign_keys.is_empty() {
        validate_foreign_keys(store, txn, schema, &new_row).await?;
    }

    handle_foreign_key_on_update(store, txn, table_name, schema, old_row, &new_row).await?;

    let pks = schema.get_pk_values(old_row);
    for index in &schema.indexes {
        let old_idx = schema.get_index_values(index, old_row);
        store
            .delete_index_entry(txn, schema.table_id, index.id, &old_idx, &pks, index.unique)
            .await?;
    }
    store.upsert(txn, table_name, new_row.clone()).await?;
    for index in &schema.indexes {
        let new_idx = schema.get_index_values(index, &new_row);
        store
            .create_index_entry(txn, schema.table_id, index.id, &new_idx, &pks, index.unique)
            .await?;
    }
    Ok(new_row)
}

pub fn prepare_insert_row(
    schema: &TableSchema,
    columns: &[Ident],
    exprs: &[Expr],
) -> Result<(Vec<Value>, Vec<usize>)> {
    let mut row_vals = vec![Value::Null; schema.columns.len()];
    let mut indices = Vec::new();

    if columns.is_empty() {
        if exprs.len() != schema.columns.len() {
            return Err(anyhow!("Column count mismatch"));
        }
        for (i, e) in exprs.iter().enumerate() {
            row_vals[i] = eval_expr(e, None, None)?;
            indices.push(i);
        }
    } else {
        if columns.len() != exprs.len() {
            return Err(anyhow!("Count mismatch"));
        }
        for (i, c) in columns.iter().enumerate() {
            let idx = schema
                .column_index(&c.value)
                .ok_or_else(|| anyhow!("Unknown col"))?;
            row_vals[idx] = eval_expr(&exprs[i], None, None)?;
            indices.push(idx);
        }
    }

    Ok((row_vals, indices))
}

pub async fn fill_missing_columns(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    schema: &TableSchema,
    row_vals: &mut Vec<Value>,
    indices: &[usize],
) -> Result<()> {
    for (i, c) in schema.columns.iter().enumerate() {
        if !indices.contains(&i) {
            if c.is_serial {
                row_vals[i] = Value::Int32(store.next_sequence_value(txn, schema.table_id).await?);
            } else if let Some(def) = &c.default_expr {
                row_vals[i] = eval_default_expr(def)?;
            } else if !c.nullable {
                return Err(anyhow!("Column '{}' cannot be null", c.name));
            }
        }
    }
    Ok(())
}

pub fn coerce_row_values(schema: &TableSchema, row_vals: &mut Vec<Value>) -> Result<()> {
    for (i, c) in schema.columns.iter().enumerate() {
        let coerced = coerce_value_for_column(row_vals[i].clone(), c)?;
        if coerced == Value::Null && !c.nullable {
            return Err(anyhow!("Column '{}' cannot be null", c.name));
        }
        row_vals[i] = coerced;
    }
    Ok(())
}

pub fn validate_check_constraints(schema: &TableSchema, row: &Row) -> Result<()> {
    let dialect = PostgreSqlDialect {};
    for check in &schema.check_constraints {
        let expr = Parser::new(&dialect)
            .try_with_sql(&check.expr)
            .and_then(|mut p| p.parse_expr())
            .map_err(|e| anyhow!("Invalid CHECK expression '{}': {}", check.expr, e))?;

        let result = eval_expr(&expr, Some(row), Some(schema))?;

        match result {
            Value::Boolean(true) => {}
            Value::Boolean(false) => {
                let name = check
                    .name
                    .as_ref()
                    .map(|n| format!("\"{}\"", n))
                    .unwrap_or_else(|| format!("({})", check.expr));
                return Err(anyhow!("new row violates check constraint {}", name));
            }
            Value::Null => {} // PostgreSQL: NULL satisfies CHECK constraints
            _ => {
                return Err(anyhow!(
                    "CHECK constraint must evaluate to boolean, got {:?}",
                    result
                ));
            }
        }
    }
    Ok(())
}

pub async fn validate_foreign_keys(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    schema: &TableSchema,
    row: &Row,
) -> Result<()> {
    for fk in &schema.foreign_keys {
        let mut fk_values: Vec<Value> = Vec::with_capacity(fk.columns.len());
        let mut all_null = true;

        for col_name in &fk.columns {
            let col_idx = schema
                .column_index(col_name)
                .ok_or_else(|| anyhow!("FK column '{}' not found in schema", col_name))?;
            let val = row.values[col_idx].clone();
            if val != Value::Null {
                all_null = false;
            }
            fk_values.push(val);
        }

        if all_null {
            continue;
        }

        let ref_schema = store.get_schema(txn, &fk.ref_table).await?.ok_or_else(|| {
            anyhow!(
                "Referenced table '{}' not found for foreign key '{}'",
                fk.ref_table,
                fk.name
            )
        })?;

        let ref_rows = store
            .batch_get_rows(
                txn,
                ref_schema.table_id,
                vec![fk_values.clone()],
                &ref_schema,
            )
            .await?;

        if ref_rows.is_empty() {
            let cols = fk.columns.join(", ");
            let vals: Vec<String> = fk_values.iter().map(|v| format!("{}", v)).collect();
            return Err(anyhow!(
                "insert or update on table \"{}\" violates foreign key constraint \"{}\"\n\
                 DETAIL:  Key ({})=({}) is not present in table \"{}\".",
                schema.name,
                fk.name,
                cols,
                vals.join(", "),
                fk.ref_table
            ));
        }
    }
    Ok(())
}

pub fn validate_update_columns(
    schema: &TableSchema,
    assignments: &[Assignment],
) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    for a in assignments {
        let c = a.id.last().unwrap().value.clone();
        let idx = schema
            .column_index(&c)
            .ok_or_else(|| anyhow!("Col not found"))?;
        if schema.pk_indices.contains(&idx) {
            return Err(anyhow!("Cannot update PK"));
        }
        indices.push(idx);
    }
    Ok(indices)
}

pub fn compute_update_values(
    schema: &TableSchema,
    old_row: &Row,
    assignments: &[Assignment],
    indices: &[usize],
    eval_row: Option<(&Row, &TableSchema)>,
) -> Result<Vec<Value>> {
    let mut vals = old_row.values.clone();
    for (i, a) in assignments.iter().enumerate() {
        let raw_val = if let Some((combined_row, combined_schema)) = eval_row {
            eval_expr(&a.value, Some(combined_row), Some(combined_schema))?
        } else {
            eval_expr(&a.value, Some(old_row), Some(schema))?
        };
        let col = &schema.columns[indices[i]];
        let coerced = coerce_value_for_column(raw_val, col)?;
        if coerced == Value::Null && !col.nullable {
            return Err(anyhow!("Column '{}' cannot be null", col.name));
        }
        vals[indices[i]] = coerced;
    }
    Ok(vals)
}

pub fn build_update_join_context<'a>(
    main_schema: &'a TableSchema,
    main_alias: &str,
    from_schema: &'a TableSchema,
    from_alias: &str,
    main_row: &'a Row,
    from_row: &'a Row,
) -> (TableSchema, Row, HashMap<String, usize>) {
    let mut combined_col_defs: Vec<ColumnDef> = main_schema.columns.clone();
    combined_col_defs.extend(from_schema.columns.clone());

    let combined_schema = TableSchema {
        name: "joined".to_string(),
        table_id: 0,
        columns: combined_col_defs,
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    };

    let mut column_offsets: HashMap<String, usize> = HashMap::new();
    for (i, col) in main_schema.columns.iter().enumerate() {
        column_offsets.insert(format!("{}.{}", main_alias, col.name), i);
        column_offsets.insert(col.name.clone(), i);
    }
    let offset = main_schema.columns.len();
    for (i, col) in from_schema.columns.iter().enumerate() {
        column_offsets.insert(format!("{}.{}", from_alias, col.name), offset + i);
        if !column_offsets.contains_key(&col.name) {
            column_offsets.insert(col.name.clone(), offset + i);
        }
    }

    let mut combined_values = main_row.values.clone();
    combined_values.extend(from_row.values.clone());
    let combined_row = Row::new(combined_values);

    (combined_schema, combined_row, column_offsets)
}
