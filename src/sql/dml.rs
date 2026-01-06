use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use sqlparser::ast::{Assignment, Expr, Ident, OnConflictAction, OnInsert, SelectItem};
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

    let insert_result = store.insert(txn, table_name, row.clone()).await;
    match insert_result {
        Ok(()) => {
            for index in &schema.indexes {
                let idx_values = schema.get_index_values(index, &row);
                store
                    .create_index_entry(
                        txn,
                        schema.table_id,
                        index.id,
                        &idx_values,
                        &pk_values,
                        index.unique,
                    )
                    .await?;
            }
            Ok(Some(row))
        }
        Err(e) if e.to_string().contains("Duplicate primary key") => {
            match on_conflict {
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
                            let col_idx = schema.column_index(&col_name).ok_or_else(|| {
                                anyhow!("Unknown column in DO UPDATE: {}", col_name)
                            })?;
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
            }
        }
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

pub async fn execute_delete_row(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    schema: &TableSchema,
    row: &Row,
) -> Result<()> {
    let pks = schema.get_pk_values(row);
    store.delete_by_pk(txn, table_name, &pks).await?;
    for index in &schema.indexes {
        let idx_values = schema.get_index_values(index, row);
        store
            .delete_index_entry(txn, schema.table_id, index.id, &idx_values, &pks, index.unique)
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
