use std::sync::Arc;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AlterTableOperation, ColumnDef as SqlColumnDef, ColumnOption, Expr, GeneratedAs, ObjectName,
    OrderByExpr, Query, TableConstraint,
};
use tikv_client::Transaction;

use super::helpers::{convert_data_type, infer_data_type, is_serial_type, normalize_ident};
use super::ExecuteResult;
use crate::storage::TikvStore;
use crate::types::{
    CheckConstraint, ColumnDef, DataType, ForeignKeyAction, ForeignKeyConstraint, IndexDef, Row,
    TableSchema, Value,
};

pub async fn execute_create_table(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    name: &ObjectName,
    columns: &[SqlColumnDef],
    constraints: &[TableConstraint],
    if_not_exists: bool,
) -> Result<ExecuteResult> {
    let table_name = name
        .0
        .last()
        .map(normalize_ident)
        .ok_or_else(|| anyhow!("Invalid table name"))?;

    if if_not_exists && store.table_exists(txn, &table_name).await? {
        return Ok(ExecuteResult::CreateTable { table_name });
    }

    let pk_columns: Vec<String> = constraints
        .iter()
        .filter_map(|c| match c {
            TableConstraint::Unique {
                columns,
                is_primary,
                ..
            } if *is_primary => Some(columns.iter().map(normalize_ident).collect::<Vec<_>>()),
            _ => None,
        })
        .flatten()
        .collect();

    let mut check_constraints: Vec<CheckConstraint> = constraints
        .iter()
        .filter_map(|c| match c {
            TableConstraint::Check { name, expr } => Some(CheckConstraint {
                name: name.as_ref().map(|n| n.value.clone()),
                expr: expr.to_string(),
            }),
            _ => None,
        })
        .collect();

    let mut col_defs = Vec::new();
    for col in columns {
        let col_name = normalize_ident(&col.name);
        let (data_type, mut is_serial) = if is_serial_type(&col.data_type) {
            (DataType::Int32, true)
        } else {
            (convert_data_type(&col.data_type)?, false)
        };

        let mut is_pk = pk_columns.contains(&col_name);
        let mut nullable = true;
        let mut unique = false;
        let mut default_expr = None;

        for opt in &col.options {
            match &opt.option {
                ColumnOption::Unique { is_primary, .. } => {
                    if *is_primary {
                        is_pk = true;
                    } else {
                        unique = true;
                    }
                }
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => default_expr = Some(expr.to_string()),
                ColumnOption::Check(expr) => {
                    check_constraints.push(CheckConstraint {
                        name: None,
                        expr: expr.to_string(),
                    });
                }
                ColumnOption::Generated {
                    generated_as,
                    generation_expr: None,
                    ..
                } => {
                    if matches!(generated_as, GeneratedAs::Always | GeneratedAs::ByDefault) {
                        is_serial = true;
                    }
                }
                _ => {}
            }
        }

        if is_serial {
            nullable = false;
        }
        if is_pk {
            nullable = false;
        }

        col_defs.push(ColumnDef {
            name: col_name,
            data_type,
            nullable,
            primary_key: is_pk,
            unique,
            is_serial,
            default_expr,
        });
    }

    let mut pk_indices = Vec::new();
    if !pk_columns.is_empty() {
        for pk_name in &pk_columns {
            if let Some(idx) = col_defs.iter().position(|c| c.name == *pk_name) {
                pk_indices.push(idx);
            }
        }
    } else {
        for (i, col) in col_defs.iter().enumerate() {
            if col.primary_key {
                pk_indices.push(i);
            }
        }
    }

    let table_id = store.next_table_id(txn).await?;

    let mut indexes = Vec::new();
    let mut next_index_id = 1u64;

    for col in col_defs.iter() {
        if col.unique && !col.primary_key {
            indexes.push(IndexDef {
                name: format!("{}_{}_key", table_name, col.name),
                id: next_index_id,
                columns: vec![col.name.clone()],
                unique: true,
            });
            next_index_id += 1;
        }
    }

    let mut foreign_keys = Vec::new();

    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
                ..
            } => {
                if !*is_primary {
                    let col_names: Vec<String> = columns.iter().map(normalize_ident).collect();
                    let idx_name = name
                        .as_ref()
                        .map(|n| n.value.clone())
                        .unwrap_or_else(|| format!("{}_{}_key", table_name, col_names.join("_")));
                    indexes.push(IndexDef {
                        name: idx_name,
                        id: next_index_id,
                        columns: col_names,
                        unique: true,
                    });
                    next_index_id += 1;
                }
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                let fk_cols: Vec<String> = columns.iter().map(normalize_ident).collect();
                let ref_table = foreign_table
                    .0
                    .last()
                    .map(normalize_ident)
                    .unwrap_or_default();
                let ref_cols: Vec<String> = referred_columns.iter().map(normalize_ident).collect();
                let fk_name = name
                    .as_ref()
                    .map(|n| n.value.clone())
                    .unwrap_or_else(|| format!("{}_{}_fkey", table_name, fk_cols.join("_")));

                let parse_action =
                    |action: &Option<sqlparser::ast::ReferentialAction>| -> ForeignKeyAction {
                        match action {
                            Some(sqlparser::ast::ReferentialAction::Cascade) => {
                                ForeignKeyAction::Cascade
                            }
                            Some(sqlparser::ast::ReferentialAction::SetNull) => {
                                ForeignKeyAction::SetNull
                            }
                            Some(sqlparser::ast::ReferentialAction::SetDefault) => {
                                ForeignKeyAction::SetDefault
                            }
                            Some(sqlparser::ast::ReferentialAction::Restrict) => {
                                ForeignKeyAction::Restrict
                            }
                            Some(sqlparser::ast::ReferentialAction::NoAction) | None => {
                                ForeignKeyAction::NoAction
                            }
                        }
                    };

                foreign_keys.push(ForeignKeyConstraint {
                    name: fk_name,
                    columns: fk_cols,
                    ref_table,
                    ref_columns: ref_cols,
                    on_delete: parse_action(on_delete),
                    on_update: parse_action(on_update),
                });
            }
            _ => {}
        }
    }

    let schema = TableSchema {
        name: table_name.clone(),
        table_id,
        columns: col_defs,
        version: 1,
        pk_indices,
        indexes,
        check_constraints,
        foreign_keys,
    };
    store.create_table(txn, schema).await?;

    Ok(ExecuteResult::CreateTable { table_name })
}

pub async fn create_table_from_query_result(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    if_not_exists: bool,
    result_cols: Vec<String>,
    result_rows: Vec<Row>,
    explicit_columns: &[SqlColumnDef],
) -> Result<ExecuteResult> {
    if if_not_exists && store.table_exists(txn, table_name).await? {
        return Ok(ExecuteResult::CreateTable {
            table_name: table_name.to_string(),
        });
    }

    // Add synthetic _rowid column as primary key (allows UPDATE/DELETE on tables without explicit PK)
    let mut col_defs: Vec<ColumnDef> = vec![ColumnDef {
        name: "_rowid".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        primary_key: true,
        unique: true,
        is_serial: true,
        default_expr: None,
    }];

    if explicit_columns.is_empty() {
        col_defs.extend(result_cols.iter().enumerate().map(|(i, col_name)| {
            let data_type = if !result_rows.is_empty() {
                infer_data_type(&result_rows[0].values[i])
            } else {
                DataType::Text
            };
            ColumnDef {
                name: col_name.clone(),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                is_serial: false,
                default_expr: None,
            }
        }));
    } else {
        col_defs.extend(explicit_columns.iter().map(|col| {
            let data_type = convert_data_type(&col.data_type).unwrap_or(DataType::Text);
            ColumnDef {
                name: normalize_ident(&col.name),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                is_serial: false,
                default_expr: None,
            }
        }));
    }

    let table_id = store.next_table_id(txn).await?;
    let schema = TableSchema {
        name: table_name.to_string(),
        table_id,
        columns: col_defs,
        version: 1,
        pk_indices: vec![0],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    };
    store.create_table(txn, schema.clone()).await?;

    let row_count = result_rows.len();
    for (i, row) in result_rows.into_iter().enumerate() {
        let mut values = vec![Value::Int64((i + 1) as i64)];
        values.extend(row.values);
        store.upsert(txn, table_name, Row::new(values)).await?;
    }

    if row_count > 0 {
        store
            .set_sequence_value(txn, schema.table_id, row_count as u64)
            .await?;
    }

    Ok(ExecuteResult::CreateTable {
        table_name: table_name.to_string(),
    })
}

pub async fn create_table_from_select_into(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
    result_cols: Vec<String>,
    result_rows: Vec<Row>,
) -> Result<ExecuteResult> {
    if store.table_exists(txn, table_name).await? {
        return Err(anyhow!("relation \"{}\" already exists", table_name));
    }

    // Add synthetic _rowid column as primary key (allows UPDATE/DELETE on tables without explicit PK)
    let mut col_defs: Vec<ColumnDef> = vec![ColumnDef {
        name: "_rowid".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        primary_key: true,
        unique: true,
        is_serial: true,
        default_expr: None,
    }];

    col_defs.extend(result_cols.iter().enumerate().map(|(i, col_name)| {
        let data_type = if !result_rows.is_empty() {
            infer_data_type(&result_rows[0].values[i])
        } else {
            DataType::Text
        };
        ColumnDef {
            name: col_name.clone(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
            is_serial: false,
            default_expr: None,
        }
    }));

    let table_id = store.next_table_id(txn).await?;
    let schema = TableSchema {
        name: table_name.to_string(),
        table_id,
        columns: col_defs,
        version: 1,
        pk_indices: vec![0],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    };
    store.create_table(txn, schema.clone()).await?;

    let row_count = result_rows.len();
    for (i, row) in result_rows.into_iter().enumerate() {
        let mut values = vec![Value::Int64((i + 1) as i64)];
        values.extend(row.values);
        store.upsert(txn, table_name, Row::new(values)).await?;
    }

    if row_count > 0 {
        store
            .set_sequence_value(txn, schema.table_id, row_count as u64)
            .await?;
    }

    Ok(ExecuteResult::Insert {
        affected_rows: row_count as u64,
    })
}

pub async fn execute_create_index(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    idx_name: &str,
    table_name: &ObjectName,
    columns: &[OrderByExpr],
    unique: bool,
    if_not_exists: bool,
    rows: Vec<Row>,
) -> Result<ExecuteResult> {
    let idx_name_str = idx_name.to_lowercase();
    let tbl_name = table_name.0.last().map(normalize_ident).unwrap();

    let mut schema = store
        .get_schema(txn, &tbl_name)
        .await?
        .ok_or_else(|| anyhow!("Table not found"))?;

    if schema.indexes.iter().any(|i| i.name == idx_name_str) {
        if if_not_exists {
            return Ok(ExecuteResult::CreateIndex {
                index_name: idx_name_str,
            });
        }
        return Err(anyhow!("Index exists"));
    }

    let mut idx_cols = Vec::new();
    for col_expr in columns {
        if let Expr::Identifier(ident) = &col_expr.expr {
            let col_name = normalize_ident(ident);
            if schema.column_index(&col_name).is_none() {
                return Err(anyhow!("Column not found"));
            }
            idx_cols.push(col_name);
        } else {
            return Err(anyhow!("Index column must be identifier"));
        }
    }

    let index_id = store.next_table_id(txn).await?;
    let new_index = IndexDef {
        name: idx_name_str.clone(),
        id: index_id,
        columns: idx_cols,
        unique,
    };

    for row in rows {
        let idx_values = schema.get_index_values(&new_index, &row);
        let pk_values = schema.get_pk_values(&row);
        store
            .create_index_entry(
                txn,
                schema.table_id,
                index_id,
                &idx_values,
                &pk_values,
                unique,
            )
            .await?;
    }

    schema.indexes.push(new_index);
    store.update_schema(txn, schema).await?;

    Ok(ExecuteResult::CreateIndex {
        index_name: idx_name_str,
    })
}

pub async fn execute_create_view(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    name: &ObjectName,
    query: &Query,
    or_replace: bool,
) -> Result<ExecuteResult> {
    let view_name = name
        .0
        .last()
        .map(normalize_ident)
        .ok_or_else(|| anyhow!("Invalid view name"))?;

    if store.get_view(txn, &view_name).await?.is_some() {
        if or_replace {
            store.drop_view(txn, &view_name).await?;
        } else {
            return Err(anyhow!("View '{}' already exists", view_name));
        }
    }

    let query_str = query.to_string();
    store.create_view(txn, &view_name, &query_str).await?;

    Ok(ExecuteResult::CreateView { view_name })
}

pub async fn execute_drop_view(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<ExecuteResult> {
    let mut last = String::new();
    for name in names {
        let v = name.0.last().map(normalize_ident).unwrap();
        if !store.drop_view(txn, &v).await? && !if_exists {
            return Err(anyhow!("View '{}' does not exist", v));
        }
        last = v;
    }
    Ok(ExecuteResult::DropView { view_name: last })
}

pub async fn execute_create_materialized_view(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    name: &ObjectName,
    query: &Query,
    or_replace: bool,
    schema: TableSchema,
    rows: Vec<Row>,
) -> Result<ExecuteResult> {
    let view_name = name
        .0
        .last()
        .map(normalize_ident)
        .ok_or_else(|| anyhow!("Invalid materialized view name"))?;

    if store
        .get_materialized_view(txn, &view_name)
        .await?
        .is_some()
    {
        if or_replace {
            store.drop_materialized_view(txn, &view_name).await?;
            store.drop_table(txn, &view_name).await?;
        } else {
            return Err(anyhow!("Materialized view '{}' already exists", view_name));
        }
    }

    let query_str = query.to_string();
    store
        .create_materialized_view(txn, &view_name, &query_str)
        .await?;

    store.create_table(txn, schema).await?;
    for row in rows {
        store.insert(txn, &view_name, row).await?;
    }

    Ok(ExecuteResult::CreateMaterializedView {
        view_name: view_name.clone(),
    })
}

pub async fn execute_drop_materialized_view(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<ExecuteResult> {
    let mut last = String::new();
    for name in names {
        let v = name.0.last().map(normalize_ident).unwrap();
        let exists = store.drop_materialized_view(txn, &v).await?;
        if !exists && !if_exists {
            return Err(anyhow!("Materialized view '{}' does not exist", v));
        }
        if exists {
            store.drop_table(txn, &v).await?;
        }
        last = v;
    }
    Ok(ExecuteResult::DropMaterializedView { view_name: last })
}

pub async fn execute_refresh_materialized_view(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    name: &str,
    rows: Vec<Row>,
) -> Result<ExecuteResult> {
    let name_lower = name.to_lowercase();
    if store
        .get_materialized_view(txn, &name_lower)
        .await?
        .is_none()
    {
        return Err(anyhow!("Materialized view '{}' does not exist", name));
    }

    store.truncate_table(txn, &name_lower).await?;
    for row in rows {
        store.insert(txn, &name_lower, row).await?;
    }

    Ok(ExecuteResult::RefreshMaterializedView {
        view_name: name_lower,
    })
}

pub async fn execute_drop_table(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<ExecuteResult> {
    let mut last = String::new();
    for name in names {
        let t = name.0.last().map(normalize_ident).unwrap();
        if !store.drop_table(txn, &t).await? && !if_exists {
            return Err(anyhow!("Table '{}' does not exist", t));
        }
        last = t;
    }
    Ok(ExecuteResult::DropTable { table_name: last })
}

pub async fn execute_truncate(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &ObjectName,
) -> Result<ExecuteResult> {
    let t = table_name.0.last().map(normalize_ident).unwrap();
    if !store.truncate_table(txn, &t).await? {
        return Err(anyhow!("Table '{}' does not exist", t));
    }
    Ok(ExecuteResult::TruncateTable { table_name: t })
}

pub async fn execute_drop_index(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    idx_name: &str,
    schema: &mut TableSchema,
    _table_name: &str,
    rows: Vec<Row>,
) -> Result<Option<String>> {
    if let Some(pos) = schema.indexes.iter().position(|i| i.name == idx_name) {
        let index = schema.indexes.remove(pos);
        for row in rows {
            let idx_values = schema.get_index_values(&index, &row);
            let pk_values = schema.get_pk_values(&row);
            store
                .delete_index_entry(
                    txn,
                    schema.table_id,
                    index.id,
                    &idx_values,
                    &pk_values,
                    index.unique,
                )
                .await?;
        }
        store.update_schema(txn, schema.clone()).await?;
        return Ok(Some(idx_name.to_string()));
    }
    Ok(None)
}

pub async fn execute_alter_table(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    name: &ObjectName,
    operation: &AlterTableOperation,
    rows: Vec<Row>,
) -> Result<ExecuteResult> {
    let t = name.0.last().map(normalize_ident).unwrap();
    let mut schema = store
        .get_schema(txn, &t)
        .await?
        .ok_or_else(|| anyhow!("Table '{}' does not exist", t))?;

    match operation {
        AlterTableOperation::AddColumn { column_def, .. } => {
            let col_name = normalize_ident(&column_def.name);
            if schema.column_index(&col_name).is_some() {
                return Err(anyhow!("Column exists"));
            }
            let data_type = convert_data_type(&column_def.data_type)?;
            let mut nullable = true;
            let mut default_expr = None;
            for opt in &column_def.options {
                match &opt.option {
                    ColumnOption::NotNull => nullable = false,
                    ColumnOption::Default(expr) => default_expr = Some(expr.to_string()),
                    _ => {}
                }
            }
            if !nullable && default_expr.is_none() {
                return Err(anyhow!("Cannot add NOT NULL column without DEFAULT"));
            }
            schema.columns.push(ColumnDef {
                name: col_name,
                data_type,
                nullable,
                primary_key: false,
                unique: false,
                is_serial: false,
                default_expr,
            });
            schema.version += 1;
            store.update_schema(txn, schema).await?;
        }
        AlterTableOperation::AddConstraint(constraint) => match constraint {
            TableConstraint::Unique {
                columns,
                is_primary,
                ..
            } if *is_primary => {
                let pk_names: Vec<String> = columns.iter().map(normalize_ident).collect();
                let mut pk_indices = Vec::new();
                for pk_name in &pk_names {
                    if let Some(idx) = schema.columns.iter().position(|c| c.name == *pk_name) {
                        schema.columns[idx].primary_key = true;
                        schema.columns[idx].nullable = false;
                        pk_indices.push(idx);
                    } else {
                        return Err(anyhow!("Column '{}' does not exist", pk_name));
                    }
                }
                schema.pk_indices = pk_indices;
                schema.version += 1;
                store.update_schema(txn, schema).await?;
            }
            TableConstraint::Unique { columns, .. } => {
                for col in columns {
                    let col_name = normalize_ident(col);
                    if let Some(idx) = schema.columns.iter().position(|c| c.name == col_name) {
                        schema.columns[idx].unique = true;
                    }
                }
                schema.version += 1;
                store.update_schema(txn, schema).await?;
            }
            TableConstraint::ForeignKey { .. } | TableConstraint::Check { .. } => {}
            _ => {}
        },
        AlterTableOperation::DropConstraint { .. } => {}
        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            ..
        } => {
            let col_name = normalize_ident(column_name);
            let col_idx = schema.column_index(&col_name);
            match col_idx {
                Some(idx) => {
                    if schema.pk_indices.contains(&idx) {
                        return Err(anyhow!("Cannot drop primary key column '{}'", col_name));
                    }
                    for index in &schema.indexes {
                        if index.columns.contains(&col_name) {
                            return Err(anyhow!(
                                "Cannot drop column '{}' used in index '{}'",
                                col_name,
                                index.name
                            ));
                        }
                    }
                    for row in rows {
                        let _pk_values = schema.get_pk_values(&row);
                        let mut new_values = row.values.clone();
                        new_values.remove(idx);
                        let new_row = Row::new(new_values);
                        store.upsert(txn, &t, new_row).await?;
                    }
                    schema.columns.remove(idx);
                    for pk_idx in &mut schema.pk_indices {
                        if *pk_idx > idx {
                            *pk_idx -= 1;
                        }
                    }
                    schema.version += 1;
                    store.update_schema(txn, schema).await?;
                }
                None => {
                    if !if_exists {
                        return Err(anyhow!("Column '{}' does not exist", col_name));
                    }
                }
            }
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            let old_name = normalize_ident(old_column_name);
            let new_name = normalize_ident(new_column_name);
            let col_idx = schema
                .column_index(&old_name)
                .ok_or_else(|| anyhow!("Column '{}' does not exist", old_name))?;
            if schema.column_index(&new_name).is_some() {
                return Err(anyhow!("Column '{}' already exists", new_name));
            }
            schema.columns[col_idx].name = new_name.clone();
            for index in &mut schema.indexes {
                for col in &mut index.columns {
                    if *col == old_name {
                        *col = new_name.clone();
                    }
                }
            }
            schema.version += 1;
            store.update_schema(txn, schema).await?;
        }
        AlterTableOperation::RenameTable { .. } => {}
        _ => return Err(anyhow!("Unsupported ALTER")),
    }

    Ok(ExecuteResult::AlterTable { table_name: t })
}
