use std::sync::Arc;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AlterTableOperation, ColumnDef as SqlColumnDef, ColumnOption, Expr, ObjectName,
    OrderByExpr, Query, TableConstraint,
};
use tikv_client::Transaction;

use super::helpers::{convert_data_type, infer_data_type, is_serial_type};
use super::ExecuteResult;
use crate::storage::TikvStore;
use crate::types::{ColumnDef, DataType, IndexDef, Row, TableSchema};

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
        .ok_or_else(|| anyhow!("Invalid table name"))?
        .value
        .clone();

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
            } if *is_primary => Some(columns.iter().map(|c| c.value.clone()).collect::<Vec<_>>()),
            _ => None,
        })
        .flatten()
        .collect();

    let mut col_defs = Vec::new();
    for col in columns {
        let col_name = col.name.value.clone();
        let (data_type, is_serial) = if is_serial_type(&col.data_type) {
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
    let schema = TableSchema {
        name: table_name.clone(),
        table_id,
        columns: col_defs,
        version: 1,
        pk_indices,
        indexes: Vec::new(),
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

    let col_defs: Vec<ColumnDef> = if explicit_columns.is_empty() {
        result_cols
            .iter()
            .enumerate()
            .map(|(i, col_name)| {
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
            })
            .collect()
    } else {
        explicit_columns
            .iter()
            .map(|col| {
                let data_type = convert_data_type(&col.data_type).unwrap_or(DataType::Text);
                ColumnDef {
                    name: col.name.value.clone(),
                    data_type,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    is_serial: false,
                    default_expr: None,
                }
            })
            .collect()
    };

    let table_id = store.next_table_id(txn).await?;
    let schema = TableSchema {
        name: table_name.to_string(),
        table_id,
        columns: col_defs,
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
    };
    store.create_table(txn, schema).await?;

    for row in result_rows {
        store.upsert(txn, table_name, row).await?;
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

    let col_defs: Vec<ColumnDef> = result_cols
        .iter()
        .enumerate()
        .map(|(i, col_name)| {
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
        })
        .collect();

    let table_id = store.next_table_id(txn).await?;
    let schema = TableSchema {
        name: table_name.to_string(),
        table_id,
        columns: col_defs,
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
    };
    store.create_table(txn, schema).await?;

    let row_count = result_rows.len();
    for row in result_rows {
        store.upsert(txn, table_name, row).await?;
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
    let idx_name_str = idx_name.to_string();
    let tbl_name = table_name.0.last().unwrap().value.clone();

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
            let col_name = ident.value.clone();
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
            .create_index_entry(txn, schema.table_id, index_id, &idx_values, &pk_values, unique)
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
        .ok_or_else(|| anyhow!("Invalid view name"))?
        .value
        .clone();
    let view_name_lower = view_name.to_lowercase();

    if store.get_view(txn, &view_name_lower).await?.is_some() {
        if or_replace {
            store.drop_view(txn, &view_name_lower).await?;
        } else {
            return Err(anyhow!("View '{}' already exists", view_name));
        }
    }

    let query_str = query.to_string();
    store.create_view(txn, &view_name_lower, &query_str).await?;

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
        let v = name.0.last().unwrap().value.to_lowercase();
        if !store.drop_view(txn, &v).await? && !if_exists {
            return Err(anyhow!("View '{}' does not exist", v));
        }
        last = v;
    }
    Ok(ExecuteResult::DropView { view_name: last })
}

pub async fn execute_drop_table(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<ExecuteResult> {
    let mut last = String::new();
    for name in names {
        let t = name.0.last().unwrap().value.clone();
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
    let t = table_name.0.last().unwrap().value.clone();
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
    let t = name.0.last().unwrap().value.clone();
    let mut schema = store
        .get_schema(txn, &t)
        .await?
        .ok_or_else(|| anyhow!("Table '{}' does not exist", t))?;

    match operation {
        AlterTableOperation::AddColumn { column_def, .. } => {
            let col_name = column_def.name.value.clone();
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
        AlterTableOperation::AddConstraint(constraint) => {
            match constraint {
                TableConstraint::Unique {
                    columns,
                    is_primary,
                    ..
                } if *is_primary => {
                    let pk_names: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
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
                        if let Some(idx) = schema.columns.iter().position(|c| c.name == col.value) {
                            schema.columns[idx].unique = true;
                        }
                    }
                    schema.version += 1;
                    store.update_schema(txn, schema).await?;
                }
                TableConstraint::ForeignKey { .. } | TableConstraint::Check { .. } => {}
                _ => {}
            }
        }
        AlterTableOperation::DropConstraint { .. } => {}
        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            ..
        } => {
            let col_name = column_name.value.clone();
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
            let old_name = old_column_name.value.clone();
            let new_name = new_column_name.value.clone();
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
