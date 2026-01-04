//! SQL executor

use super::{parse_sql, ExecuteResult, expr::{eval_expr, eval_expr_join, JoinContext}, Session, Aggregator};
use crate::storage::TikvStore;
use crate::types::{ColumnDef, DataType, Row, TableSchema, Value, IndexDef};
use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AlterTableOperation, Assignment, ColumnDef as SqlColumnDef, ColumnOption, DataType as SqlDataType, Expr, Ident,
    ObjectName, Query, SelectItem, SetExpr, Statement, TableConstraint, Values, FunctionArg, FunctionArgExpr, 
    OrderByExpr, BinaryOperator, GroupByExpr, JoinOperator, JoinConstraint, TableFactor, LockType,
    Value as SqlValue, Cte
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use tikv_client::Transaction;
use tracing::debug;

/// SQL Executor that operates on TiKV storage
pub struct Executor {
    store: Arc<TikvStore>,
}

impl Executor {
    pub fn new(store: Arc<TikvStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> Arc<TikvStore> {
        self.store.clone()
    }

    /// Execute a SQL statement string using the provided session
    pub async fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecuteResult> {
        let sql_upper = sql.trim().to_uppercase();
        if let Some(reason) = get_skip_reason(&sql_upper) {
            return Ok(ExecuteResult::Skipped { message: reason });
        }
        
        let statements = match parse_sql(sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                if let Some(reason) = get_unsupported_reason(&sql_upper) {
                    return Ok(ExecuteResult::Skipped { message: reason });
                }
                return Err(e);
            }
        };
        
        if statements.is_empty() {
            return Ok(ExecuteResult::Empty);
        }

        let stmt = &statements[0];
        debug!("Executing statement: {:?}", stmt);

        match stmt {
            // Transaction Control
            Statement::StartTransaction { .. } => {
                session.begin().await?;
                Ok(ExecuteResult::Empty)
            },
            Statement::Commit { .. } => {
                session.commit().await?;
                Ok(ExecuteResult::Empty)
            },
            Statement::Rollback { .. } => {
                session.rollback().await?;
                Ok(ExecuteResult::Empty)
            },
            // DDL/DML - delegated to session transaction management
            _ => {
                let is_autocommit = !session.is_in_transaction();
                
                if is_autocommit {
                    session.begin().await?;
                }

                let res = async {
                    let txn = session.get_mut_txn().expect("Transaction must be active");
                    self.execute_statement_on_txn(txn, stmt).await
                }.await;

                if is_autocommit {
                    if res.is_ok() {
                        session.commit().await?;
                    } else {
                        session.rollback().await?;
                    }
                }
                
                res
            }
        }
    }

    /// Execute a parsed SQL statement on a given transaction
    async fn execute_statement_on_txn(&self, txn: &mut Transaction, stmt: &Statement) -> Result<ExecuteResult> {
        match stmt {
            Statement::CreateTable { name, columns, constraints, if_not_exists, .. } => {
                self.execute_create_table(txn, name, columns, constraints, *if_not_exists).await
            }
            Statement::CreateIndex { name, table_name, columns, unique, if_not_exists, .. } => {
                let index_name = name.as_ref().ok_or_else(|| anyhow!("Index name required"))?;
                let idx_name_str = index_name.0.last().unwrap().value.as_str();
                self.execute_create_index(txn, idx_name_str, table_name, columns, *unique, *if_not_exists).await
            }
            Statement::Drop { object_type, names, if_exists, .. } => {
                use sqlparser::ast::ObjectType;
                match object_type {
                    ObjectType::Table => self.execute_drop_table(txn, names, *if_exists).await,
                    ObjectType::View | ObjectType::Sequence | ObjectType::Index => Ok(ExecuteResult::Empty),
                    _ => Ok(ExecuteResult::Empty),
                }
            }
            Statement::Truncate { table_name, .. } => {
                self.execute_truncate(txn, table_name).await
            }
            Statement::AlterTable { name, operations, .. } => {
                for op in operations {
                    self.execute_alter_table(txn, name, op).await?;
                }
                let table_name = name.0.last().unwrap().value.clone();
                Ok(ExecuteResult::AlterTable { table_name })
            }
            Statement::Insert { table_name, columns, source, returning, .. } => {
                self.execute_insert(txn, table_name, columns, source, returning).await
            }
            Statement::Delete { from, selection, returning, .. } => {
                self.execute_delete(txn, from, selection, returning).await
            }
            Statement::Update { table, assignments, selection, returning, .. } => {
                self.execute_update(txn, table, assignments, selection, returning).await
            }
            Statement::Query(query) => self.execute_query(txn, query).await,
            Statement::ShowTables { .. } => self.execute_show_tables(txn).await,
            Statement::SetVariable { .. } | Statement::SetTimeZone { .. } | Statement::SetNames { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::CreateType { .. } | Statement::CreateFunction { .. } | Statement::CreateProcedure { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::CreateSequence { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::CreateView { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::AlterIndex { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::Grant { .. } | Statement::Revoke { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::Comment { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::Copy { .. } => {
                Ok(ExecuteResult::Empty)
            }
            _ => Err(anyhow!("Unsupported statement: {:?}", stmt)),
        }
    }

    // --- All DDL/DML methods ---

    async fn execute_create_table(&self, txn: &mut Transaction, name: &ObjectName, columns: &[SqlColumnDef], constraints: &[TableConstraint], if_not_exists: bool) -> Result<ExecuteResult> {
        let table_name = name.0.last().ok_or_else(|| anyhow!("Invalid table name"))?.value.clone();
        if if_not_exists {
            if self.store.table_exists(txn, &table_name).await? { return Ok(ExecuteResult::CreateTable { table_name }); }
        }
        let pk_columns: Vec<String> = constraints.iter().filter_map(|c| {
            match c {
                TableConstraint::Unique { columns, is_primary, .. } if *is_primary => Some(columns.iter().map(|c| c.value.clone()).collect::<Vec<String>>()),
                _ => None
            }
        }).flatten().collect();
        let mut col_defs = Vec::new();
        for col in columns {
            let col_name = col.name.value.clone();
            let (data_type, is_serial) = if is_serial_type(&col.data_type) { (DataType::Int32, true) } else { (convert_data_type(&col.data_type)?, false) };
            let mut is_pk = pk_columns.contains(&col_name);
            let mut nullable = true;
            let mut unique = false;
            let mut default_expr = None;
            for opt in &col.options {
                match &opt.option {
                    ColumnOption::Unique { is_primary, .. } => { if *is_primary { is_pk = true; } else { unique = true; } }
                    ColumnOption::NotNull => nullable = false,
                    ColumnOption::Default(expr) => default_expr = Some(expr.to_string()),
                    _ => {}
                }
            }
            if is_serial { nullable = false; }
            if is_pk { nullable = false; }
            col_defs.push(ColumnDef { name: col_name, data_type, nullable, primary_key: is_pk, unique, is_serial, default_expr });
        }
        let mut pk_indices = Vec::new();
        if !pk_columns.is_empty() {
            for pk_name in &pk_columns {
                 if let Some(idx) = col_defs.iter().position(|c| c.name == *pk_name) { pk_indices.push(idx); }
            }
        } else {
            for (i, col) in col_defs.iter().enumerate() { if col.primary_key { pk_indices.push(i); } }
        }
        let table_id = self.store.next_table_id(txn).await?;
        let schema = TableSchema { name: table_name.clone(), table_id, columns: col_defs, version: 1, pk_indices, indexes: Vec::new() };
        self.store.create_table(txn, schema).await?;
        Ok(ExecuteResult::CreateTable { table_name })
    }

    async fn execute_create_index(&self, txn: &mut Transaction, idx_name: &str, table_name: &ObjectName, columns: &[OrderByExpr], unique: bool, if_not_exists: bool) -> Result<ExecuteResult> {
        let idx_name_str = idx_name.to_string();
        let tbl_name = table_name.0.last().unwrap().value.clone();
        let mut schema = self.store.get_schema(txn, &tbl_name).await?.ok_or_else(|| anyhow!("Table not found"))?;
        if schema.indexes.iter().any(|i| i.name == idx_name_str) {
            if if_not_exists { return Ok(ExecuteResult::CreateIndex { index_name: idx_name_str }); }
            return Err(anyhow!("Index exists"));
        }
        let mut idx_cols = Vec::new();
        for col_expr in columns {
            if let Expr::Identifier(ident) = &col_expr.expr {
                let col_name = ident.value.clone();
                if schema.column_index(&col_name).is_none() { return Err(anyhow!("Column not found")); }
                idx_cols.push(col_name);
            } else { return Err(anyhow!("Index column must be identifier")); }
        }
        let index_id = self.store.next_table_id(txn).await?;
        let new_index = IndexDef { name: idx_name_str.clone(), id: index_id, columns: idx_cols, unique };
        let rows = self.scan_and_fill(txn, &tbl_name, &schema).await?;
        for row in rows {
            let idx_values = schema.get_index_values(&new_index, &row);
            let pk_values = schema.get_pk_values(&row);
            self.store.create_index_entry(txn, schema.table_id, index_id, &idx_values, &pk_values, unique).await?;
        }
        schema.indexes.push(new_index);
        self.store.update_schema(txn, schema).await?;
        Ok(ExecuteResult::CreateIndex { index_name: idx_name_str })
    }

    async fn execute_drop_table(&self, txn: &mut Transaction, names: &[ObjectName], if_exists: bool) -> Result<ExecuteResult> {
        let mut last = String::new();
        for name in names {
            let t = name.0.last().unwrap().value.clone();
            if !self.store.drop_table(txn, &t).await? && !if_exists { return Err(anyhow!("Table '{}' does not exist", t)); }
            last = t;
        }
        Ok(ExecuteResult::DropTable { table_name: last })
    }

    async fn execute_truncate(&self, txn: &mut Transaction, table_name: &ObjectName) -> Result<ExecuteResult> {
        let t = table_name.0.last().unwrap().value.clone();
        if !self.store.truncate_table(txn, &t).await? { return Err(anyhow!("Table '{}' does not exist", t)); }
        Ok(ExecuteResult::TruncateTable { table_name: t })
    }

    async fn execute_alter_table(&self, txn: &mut Transaction, name: &ObjectName, operation: &AlterTableOperation) -> Result<ExecuteResult> {
        let t = name.0.last().unwrap().value.clone();
        let mut schema = self.store.get_schema(txn, &t).await?.ok_or_else(|| anyhow!("Table '{}' does not exist", t))?;
        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let col_name = column_def.name.value.clone();
                if schema.column_index(&col_name).is_some() { return Err(anyhow!("Column exists")); }
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
                if !nullable && default_expr.is_none() { return Err(anyhow!("Cannot add NOT NULL column without DEFAULT")); }
                schema.columns.push(ColumnDef { name: col_name, data_type, nullable, primary_key: false, unique: false, is_serial: false, default_expr });
                schema.version += 1;
                self.store.update_schema(txn, schema).await?;
            },
            AlterTableOperation::AddConstraint(constraint) => {
                match constraint {
                    TableConstraint::Unique { columns, is_primary, .. } if *is_primary => {
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
                        self.store.update_schema(txn, schema).await?;
                    }
                    TableConstraint::Unique { columns, .. } => {
                        for col in columns {
                            if let Some(idx) = schema.columns.iter().position(|c| c.name == col.value) {
                                schema.columns[idx].unique = true;
                            }
                        }
                        schema.version += 1;
                        self.store.update_schema(txn, schema).await?;
                    }
                    TableConstraint::ForeignKey { .. } | TableConstraint::Check { .. } => {}
                    _ => {}
                }
            },
            AlterTableOperation::DropConstraint { .. } => {},
            AlterTableOperation::DropColumn { .. } => {},
            AlterTableOperation::RenameColumn { .. } => {},
            AlterTableOperation::RenameTable { .. } => {},
            _ => return Err(anyhow!("Unsupported ALTER")),
        }
        Ok(ExecuteResult::AlterTable { table_name: t })
    }

    async fn execute_insert(&self, txn: &mut Transaction, table_name: &ObjectName, columns: &[Ident], source: &Option<Box<Query>>, returning: &Option<Vec<SelectItem>>) -> Result<ExecuteResult> {
        let t = table_name.0.last().unwrap().value.clone();
        let schema = self.store.get_schema(txn, &t).await?.ok_or_else(|| anyhow!("Table '{}' does not exist", t))?;
        let source = source.as_ref().ok_or_else(|| anyhow!("INSERT requires VALUES"))?;
        let values = match &*source.body { SetExpr::Values(Values { rows, .. }) => rows, _ => return Err(anyhow!("Only VALUES supported")) };
        let mut affected = 0;
        let mut ret_rows = Vec::new();
        let mut ret_cols = Vec::new();
        if let Some(items) = returning {
            for item in items {
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => ret_cols.push(id.value.clone()),
                    SelectItem::ExprWithAlias { alias, .. } => ret_cols.push(alias.value.clone()),
                    SelectItem::Wildcard(_) => for c in &schema.columns { ret_cols.push(c.name.clone()); },
                    _ => return Err(anyhow!("Unsupported RETURNING")),
                }
            }
        }
        for exprs in values {
            let mut row_vals = vec![Value::Null; schema.columns.len()];
            let mut indices = Vec::new();
            if columns.is_empty() {
                if exprs.len() != schema.columns.len() { return Err(anyhow!("Column count mismatch")); }
                for (i, e) in exprs.iter().enumerate() { row_vals[i] = eval_expr(e, None, None)?; indices.push(i); }
            } else {
                if columns.len() != exprs.len() { return Err(anyhow!("Count mismatch")); }
                for (i, c) in columns.iter().enumerate() {
                    let idx = schema.column_index(&c.value).ok_or_else(|| anyhow!("Unknown col"))?;
                    row_vals[idx] = eval_expr(&exprs[i], None, None)?;
                    indices.push(idx);
                }
            }
            for (i, c) in schema.columns.iter().enumerate() {
                if !indices.contains(&i) {
                    if c.is_serial {
                        row_vals[i] = Value::Int32(self.store.next_sequence_value(txn, schema.table_id).await?);
                    } else if let Some(def) = &c.default_expr {
                        row_vals[i] = eval_default_expr(def)?;
                    } else if !c.nullable { return Err(anyhow!("Column '{}' cannot be null", c.name)); }
                }
            }
            let row = Row::new(row_vals);
            self.store.insert(txn, &t, row.clone()).await?;
            let pk_values = schema.get_pk_values(&row);
            for index in &schema.indexes {
                let idx_values = schema.get_index_values(index, &row);
                self.store.create_index_entry(txn, schema.table_id, index.id, &idx_values, &pk_values, index.unique).await?;
            }
            affected += 1;
            if let Some(items) = returning {
                let mut vals = Vec::new();
                for item in items {
                    match item {
                        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => vals.push(eval_expr(e, Some(&row), Some(&schema))?),
                        SelectItem::Wildcard(_) => vals.extend(row.values.clone()),
                        _ => {}
                    }
                }
                ret_rows.push(Row::new(vals));
            }
        }
        if returning.is_some() { Ok(ExecuteResult::Select { columns: ret_cols, rows: ret_rows }) } else { Ok(ExecuteResult::Insert { affected_rows: affected }) }
    }

    async fn execute_delete(&self, txn: &mut Transaction, from: &[sqlparser::ast::TableWithJoins], selection: &Option<Expr>, returning: &Option<Vec<SelectItem>>) -> Result<ExecuteResult> {
        let t = match &from[0].relation { sqlparser::ast::TableFactor::Table { name, .. } => name.0.last().unwrap().value.clone(), _ => return Err(anyhow!("Unsupported")) };
        let schema = self.store.get_schema(txn, &t).await?.ok_or_else(|| anyhow!("Table not found"))?;
        if schema.pk_indices.is_empty() { return Err(anyhow!("No PK")); }
        let resolved_selection = if let Some(sel) = selection {
            Some(self.resolve_subqueries(txn, sel).await?)
        } else {
            None
        };
        let rows = self.scan_and_fill(txn, &t, &schema).await?;
        let mut cnt = 0;
        let mut ret_rows = Vec::new();
        let mut ret_cols = Vec::new();
        if let Some(items) = returning {
            for item in items {
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => ret_cols.push(id.value.clone()),
                    SelectItem::ExprWithAlias { alias, .. } => ret_cols.push(alias.value.clone()),
                    SelectItem::Wildcard(_) => for c in &schema.columns { ret_cols.push(c.name.clone()); },
                    _ => return Err(anyhow!("Unsupported RETURNING")),
                }
            }
        }
        for r in rows {
            if let Some(ref e) = resolved_selection {
                if !matches!(eval_expr(e, Some(&r), Some(&schema))?, Value::Boolean(true)) { continue; }
            }
            if let Some(items) = returning {
                let mut vals = Vec::new();
                for item in items {
                    match item {
                        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => vals.push(eval_expr(e, Some(&r), Some(&schema))?),
                        SelectItem::Wildcard(_) => vals.extend(r.values.clone()),
                        _ => {}
                    }
                }
                ret_rows.push(Row::new(vals));
            }
            let pks = schema.get_pk_values(&r);
            self.store.delete_by_pk(txn, &t, &pks).await?;
            for index in &schema.indexes {
                let idx_values = schema.get_index_values(index, &r);
                self.store.delete_index_entry(txn, schema.table_id, index.id, &idx_values, &pks, index.unique).await?;
            }
            cnt += 1;
        }
        if returning.is_some() { Ok(ExecuteResult::Select { columns: ret_cols, rows: ret_rows }) } else { Ok(ExecuteResult::Delete { affected_rows: cnt }) }
    }

    async fn execute_update(&self, txn: &mut Transaction, table: &sqlparser::ast::TableWithJoins, assignments: &[Assignment], selection: &Option<Expr>, returning: &Option<Vec<SelectItem>>) -> Result<ExecuteResult> {
        let t = match &table.relation { sqlparser::ast::TableFactor::Table { name, .. } => name.0.last().unwrap().value.clone(), _ => return Err(anyhow!("Unsupported")) };
        let schema = self.store.get_schema(txn, &t).await?.ok_or_else(|| anyhow!("Table not found"))?;
        if schema.pk_indices.is_empty() { return Err(anyhow!("No PK")); }
        let resolved_selection = if let Some(sel) = selection {
            Some(self.resolve_subqueries(txn, sel).await?)
        } else {
            None
        };
        let mut indices = Vec::new();
        for a in assignments {
            let c = a.id.last().unwrap().value.clone();
            let idx = schema.column_index(&c).ok_or_else(|| anyhow!("Col not found"))?;
            if schema.pk_indices.contains(&idx) { return Err(anyhow!("Cannot update PK")); }
            indices.push(idx);
        }
        let rows = self.scan_and_fill(txn, &t, &schema).await?;
        let mut cnt = 0;
        let mut ret_rows = Vec::new();
        let mut ret_cols = Vec::new();
        if let Some(items) = returning {
            for item in items {
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => ret_cols.push(id.value.clone()),
                    SelectItem::ExprWithAlias { alias, .. } => ret_cols.push(alias.value.clone()),
                    SelectItem::Wildcard(_) => for c in &schema.columns { ret_cols.push(c.name.clone()); },
                    _ => return Err(anyhow!("Unsupported RETURNING")),
                }
            }
        }
        for r in rows {
            if let Some(ref e) = resolved_selection {
                if !matches!(eval_expr(e, Some(&r), Some(&schema))?, Value::Boolean(true)) { continue; }
            }
            let mut vals = r.values.clone();
            for (i, a) in assignments.iter().enumerate() {
                vals[indices[i]] = eval_expr(&a.value, Some(&r), Some(&schema))?;
            }
            let new_row = Row::new(vals);
            let pks = schema.get_pk_values(&r);
            for index in &schema.indexes {
                let old_idx = schema.get_index_values(index, &r);
                self.store.delete_index_entry(txn, schema.table_id, index.id, &old_idx, &pks, index.unique).await?;
            }
            self.store.upsert(txn, &t, new_row.clone()).await?;
            for index in &schema.indexes {
                let new_idx = schema.get_index_values(index, &new_row);
                self.store.create_index_entry(txn, schema.table_id, index.id, &new_idx, &pks, index.unique).await?;
            }
            if let Some(items) = returning {
                let mut ret_vals = Vec::new();
                for item in items {
                    match item {
                        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => ret_vals.push(eval_expr(e, Some(&new_row), Some(&schema))?),
                        SelectItem::Wildcard(_) => ret_vals.extend(new_row.values.clone()),
                        _ => {}
                    }
                }
                ret_rows.push(Row::new(ret_vals));
            }
            cnt += 1;
        }
        if returning.is_some() { Ok(ExecuteResult::Select { columns: ret_cols, rows: ret_rows }) } else { Ok(ExecuteResult::Update { affected_rows: cnt }) }
    }

    async fn execute_query(&self, txn: &mut Transaction, query: &Query) -> Result<ExecuteResult> {
        let ctes = self.build_cte_context(txn, query).await?;
        self.execute_query_with_ctes(txn, query, &ctes).await
    }

    async fn build_cte_context(&self, txn: &mut Transaction, query: &Query) -> Result<HashMap<String, (TableSchema, Vec<Row>)>> {
        let mut ctes: HashMap<String, (TableSchema, Vec<Row>)> = HashMap::new();
        if let Some(with) = &query.with {
            if with.recursive {
                return Err(anyhow!("Recursive CTEs not yet supported"));
            }
            for cte in &with.cte_tables {
                let cte_name = cte.alias.name.value.to_lowercase();
                let cte_result = self.execute_query_with_ctes(txn, &cte.query, &ctes).await?;
                match cte_result {
                    ExecuteResult::Select { columns, rows } => {
                        let col_names: Vec<String> = if cte.alias.columns.is_empty() {
                            columns
                        } else {
                            cte.alias.columns.iter().map(|c| c.value.clone()).collect()
                        };
                        let schema = TableSchema {
                            table_id: 0,
                            name: cte_name.clone(),
                            columns: col_names.iter().map(|n| ColumnDef { 
                                name: n.clone(), 
                                data_type: DataType::Text, 
                                nullable: true, 
                                primary_key: false,
                                unique: false,
                                is_serial: false, 
                                default_expr: None 
                            }).collect(),
                            pk_indices: vec![],
                            indexes: vec![],
                            version: 1,
                        };
                        ctes.insert(cte_name, (schema, rows));
                    }
                    _ => return Err(anyhow!("CTE must be a SELECT query")),
                }
            }
        }
        Ok(ctes)
    }

    async fn execute_query_with_ctes(&self, txn: &mut Transaction, query: &Query, ctes: &HashMap<String, (TableSchema, Vec<Row>)>) -> Result<ExecuteResult> {
        let select = match &*query.body { SetExpr::Select(s) => s, _ => return Err(anyhow!("Only SELECT supported")) };
        
        if select.from.is_empty() {
            return self.execute_tableless_query(select).await;
        }
        
        let has_joins = !select.from[0].joins.is_empty();
        
        if has_joins {
            return self.execute_join_query_with_ctes(txn, query, select, ctes).await;
        }
        
        let t = match &select.from[0].relation { TableFactor::Table { name, .. } => name.0.last().unwrap().value.clone(), _ => return Err(anyhow!("Unsupported table")) };
        let t_lower = t.to_lowercase();
        
        let (schema, all_rows_base) = if let Some((cte_schema, cte_rows)) = ctes.get(&t_lower) {
            (cte_schema.clone(), cte_rows.clone())
        } else {
            let schema = self.store.get_schema(txn, &t).await?.ok_or_else(|| anyhow!("Table '{}' not found", t))?;
            let rows = self.scan_and_fill(txn, &t, &schema).await?;
            (schema, rows)
        };
        
        let resolved_selection = if let Some(sel) = &select.selection {
            Some(self.resolve_subqueries(txn, sel).await?)
        } else {
            None
        };
        
        let all_rows = if ctes.contains_key(&t_lower) {
            all_rows_base
        } else {
            let mut index_scan_rows = None;
            if let Some(ref sel) = resolved_selection {
                for index in &schema.indexes {
                    if let Some(values) = extract_eq_conditions(sel, &index.columns) {
                        debug!("Using Index Scan on {}", index.name);
                        let pks = self.store.scan_index(txn, schema.table_id, index.id, &values, index.unique).await?;
                        if !pks.is_empty() {
                            let mut rows = self.store.batch_get_rows(txn, schema.table_id, pks.clone(), &schema).await?;
                            if rows.is_empty() {
                                debug!("Index scan returned {} PKs but no rows found, falling back to full scan", pks.len());
                            } else {
                                for r in &mut rows { fill_row_defaults(r, &schema)?; }
                                index_scan_rows = Some(rows);
                            }
                        }
                        break; 
                    }
                }
            }
            index_scan_rows.unwrap_or(all_rows_base)
        };
        
        let mut filtered_rows = if let Some(ref sel) = resolved_selection {
            let mut v = Vec::new();
            for r in all_rows {
                if matches!(eval_expr(sel, Some(&r), Some(&schema))?, Value::Boolean(true)) { v.push(r); }
            }
            v
        } else { all_rows };

        let has_for_update = query.locks.iter().any(|l| matches!(l.lock_type, LockType::Update));
        if has_for_update && !filtered_rows.is_empty() {
            self.store.lock_rows(txn, &t, &filtered_rows).await?;
        }

        let group_keys_exprs = match &select.group_by {
            GroupByExpr::Expressions(exprs) => exprs,
            GroupByExpr::All => return Err(anyhow!("GROUP BY ALL not supported")),
        };
        let mut agg_funcs = Vec::new(); 
        for (i, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(Expr::Function(f)) | SelectItem::ExprWithAlias { expr: Expr::Function(f), .. } => {
                    agg_funcs.push((i, f.clone()));
                }
                _ => {}
            }
        }
        
        let is_agg = !group_keys_exprs.is_empty() || !agg_funcs.is_empty();

        if is_agg {
            let mut groups: HashMap<Vec<u8>, Vec<Aggregator>> = HashMap::new();
            let mut group_rows: HashMap<Vec<u8>, Row> = HashMap::new();

            for row in filtered_rows {
                let mut key = Vec::new();
                for expr in group_keys_exprs {
                    key.push(eval_expr(expr, Some(&row), Some(&schema))?);
                }
                let key_bytes = bincode::serialize(&key).unwrap();
                
                if !groups.contains_key(&key_bytes) {
                    let mut aggs = Vec::new();
                    for (_, f) in &agg_funcs {
                        let name = f.name.0.last().unwrap().value.clone();
                        aggs.push(Aggregator::new(&name)?);
                    }
                    groups.insert(key_bytes.clone(), aggs);
                    group_rows.insert(key_bytes.clone(), row.clone());
                }
                
                let aggs = groups.get_mut(&key_bytes).unwrap();
                for (agg_idx, (_, f)) in agg_funcs.iter().enumerate() {
                    let arg_expr = if f.args.is_empty() { None } else {
                        match &f.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
                            _ => return Err(anyhow!("Unsupported arg"))
                        }
                    };
                    
                    let val = if let Some(e) = arg_expr {
                        eval_expr(e, Some(&row), Some(&schema))?
                    } else { Value::Int32(1) };
                    aggs[agg_idx].update(&val)?;
                }
            }
            
            let mut final_rows = Vec::new();
            let col_names: Vec<String> = select.projection.iter().map(get_select_item_name).collect();

            for (key_bytes, aggs) in groups {
                let representative = &group_rows[&key_bytes];
                
                if let Some(having_expr) = &select.having {
                    let having_val = eval_having_expr(having_expr, representative, &schema, &agg_funcs, &aggs)?;
                    if !matches!(having_val, Value::Boolean(true)) {
                        continue;
                    }
                }
                
                let mut row_values = Vec::new();
                
                for (i, item) in select.projection.iter().enumerate() {
                    if let Some(agg_pos) = agg_funcs.iter().position(|(idx, _)| *idx == i) {
                        row_values.push(aggs[agg_pos].result());
                    } else {
                        let expr = match item {
                            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                            _ => return Err(anyhow!("Unsupported item")),
                        };
                        row_values.push(eval_expr(expr, Some(representative), Some(&schema))?);
                    }
                }
                final_rows.push(Row::new(row_values));
            }
            
            return Ok(ExecuteResult::Select { columns: col_names, rows: final_rows });
        }

        if !query.order_by.is_empty() {
            filtered_rows.sort_by(|a, b| {
                for order_expr in &query.order_by {
                    let val_a = eval_expr(&order_expr.expr, Some(a), Some(&schema)).unwrap_or(Value::Null);
                    let val_b = eval_expr(&order_expr.expr, Some(b), Some(&schema)).unwrap_or(Value::Null);
                    let cmp = super::expr::compare_values(&val_a, &val_b).unwrap_or(0);
                    if cmp != 0 {
                        let asc = order_expr.asc.unwrap_or(true);
                        return if asc { if cmp > 0 { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less } }
                               else { if cmp > 0 { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater } };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let mut final_rows = filtered_rows;
        if let Some(offset) = &query.offset {
             if let Ok(v) = eval_expr(&offset.value, None, None) {
                 let n = match v { Value::Int64(n) => n as usize, Value::Int32(n) => n as usize, _ => 0 };
                 final_rows = final_rows.into_iter().skip(n).collect();
             }
        }
        if let Some(limit) = &query.limit {
             if let Ok(v) = eval_expr(limit, None, None) {
                 let n = match v { Value::Int64(n) => n as usize, Value::Int32(n) => n as usize, _ => usize::MAX };
                 final_rows = final_rows.into_iter().take(n).collect();
             }
        }

        let mut cols = Vec::new();
        let mut indices = Vec::new();
        let wildcard = select.projection.iter().any(|p| matches!(p, SelectItem::Wildcard(_)));
        if wildcard {
            for c in &schema.columns { cols.push(c.name.clone()); }
            for i in 0..schema.columns.len() { indices.push(i); }
        } else {
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                        let idx = schema.column_index(&id.value).ok_or_else(|| anyhow!("Unknown col"))?;
                        cols.push(id.value.clone()); indices.push(idx);
                    },
                    SelectItem::ExprWithAlias { expr: Expr::Identifier(id), alias } => {
                        let idx = schema.column_index(&id.value).ok_or_else(|| anyhow!("Unknown col"))?;
                        cols.push(alias.value.clone()); indices.push(idx);
                    },
                    _ => return Err(anyhow!("Unsupported select item"))
                }
            }
        }

        let mut rows: Vec<Row> = final_rows.into_iter().map(|r| {
            if wildcard { r } else {
                Row::new(indices.iter().map(|&i| r.values[i].clone()).collect())
            }
        }).collect();

        if select.distinct.is_some() {
            rows = dedup_rows(rows);
        }

        Ok(ExecuteResult::Select { columns: cols, rows })
    }

    async fn execute_tableless_query(&self, select: &sqlparser::ast::Select) -> Result<ExecuteResult> {
        let mut cols = Vec::new();
        let mut values = Vec::new();
        
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    cols.push("?column?".to_string());
                    values.push(eval_expr(expr, None, None)?);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    cols.push(alias.value.clone());
                    values.push(eval_expr(expr, None, None)?);
                }
                _ => return Err(anyhow!("Unsupported select item in tableless query")),
            }
        }
        
        Ok(ExecuteResult::Select { columns: cols, rows: vec![Row::new(values)] })
    }

    async fn execute_join_query(&self, txn: &mut Transaction, query: &Query, select: &sqlparser::ast::Select) -> Result<ExecuteResult> {
        self.execute_join_query_with_ctes(txn, query, select, &HashMap::new()).await
    }

    async fn get_table_data(&self, txn: &mut Transaction, table_name: &str, ctes: &HashMap<String, (TableSchema, Vec<Row>)>) -> Result<(TableSchema, Vec<Row>)> {
        let t_lower = table_name.to_lowercase();
        if let Some((schema, rows)) = ctes.get(&t_lower) {
            Ok((schema.clone(), rows.clone()))
        } else {
            let schema = self.store.get_schema(txn, table_name).await?
                .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;
            let rows = self.scan_and_fill(txn, table_name, &schema).await?;
            Ok((schema, rows))
        }
    }

    async fn execute_join_query_with_ctes(&self, txn: &mut Transaction, query: &Query, select: &sqlparser::ast::Select, ctes: &HashMap<String, (TableSchema, Vec<Row>)>) -> Result<ExecuteResult> {
        let (base_table, base_alias) = match &select.from[0].relation {
            TableFactor::Table { name, alias, .. } => {
                let tbl = name.0.last().unwrap().value.clone();
                let als = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| tbl.clone());
                (tbl, als)
            },
            _ => return Err(anyhow!("Unsupported base table")),
        };
        
        let (base_schema, base_rows) = self.get_table_data(txn, &base_table, ctes).await?;

        let mut combined_schemas: Vec<(String, TableSchema)> = vec![(base_alias.clone(), base_schema.clone())];
        let mut combined_rows: Vec<Row> = base_rows;

        for join in &select.from[0].joins {
            let (join_table, join_alias) = match &join.relation {
                TableFactor::Table { name, alias, .. } => {
                    let tbl = name.0.last().unwrap().value.clone();
                    let als = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| tbl.clone());
                    (tbl, als)
                },
                _ => return Err(anyhow!("Unsupported join table")),
            };

            let (join_schema, join_rows) = self.get_table_data(txn, &join_table, ctes).await?;

            let join_condition = match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr)) => Some(expr),
                JoinOperator::LeftOuter(JoinConstraint::On(expr)) => Some(expr),
                JoinOperator::RightOuter(JoinConstraint::On(expr)) => Some(expr),
                JoinOperator::CrossJoin => None,
                _ => return Err(anyhow!("Unsupported JOIN type")),
            };

            let is_left_join = matches!(&join.join_operator, JoinOperator::LeftOuter(_));
            
            let mut column_offsets: HashMap<String, usize> = HashMap::new();
            let mut offset = 0;
            for (alias, schema) in &combined_schemas {
                for col in &schema.columns {
                    column_offsets.insert(format!("{}.{}", alias, col.name), offset);
                    if !column_offsets.contains_key(&col.name) {
                        column_offsets.insert(col.name.clone(), offset);
                    }
                    offset += 1;
                }
            }
            let join_start_offset = offset;
            for col in &join_schema.columns {
                column_offsets.insert(format!("{}.{}", join_alias, col.name), offset);
                if !column_offsets.contains_key(&col.name) {
                    column_offsets.insert(col.name.clone(), offset);
                }
                offset += 1;
            }

            let mut combined_col_defs: Vec<ColumnDef> = Vec::new();
            for (_, schema) in &combined_schemas {
                combined_col_defs.extend(schema.columns.clone());
            }
            combined_col_defs.extend(join_schema.columns.clone());
            let temp_combined_schema = TableSchema {
                name: "joined".to_string(),
                table_id: 0,
                columns: combined_col_defs,
                version: 1,
                pk_indices: vec![],
                indexes: vec![],
            };

            let mut new_combined_rows = Vec::new();
            for left_row in &combined_rows {
                let mut matched = false;
                for right_row in &join_rows {
                    let mut combined_values = left_row.values.clone();
                    combined_values.extend(right_row.values.clone());
                    let combined_row = Row::new(combined_values);

                    let matches = if let Some(cond) = join_condition {
                        let ctx = JoinContext {
                            tables: HashMap::new(),
                            column_offsets: column_offsets.clone(),
                            combined_row: &combined_row,
                            combined_schema: &temp_combined_schema,
                        };
                        matches!(eval_expr_join(cond, &ctx)?, Value::Boolean(true))
                    } else {
                        true
                    };

                    if matches {
                        new_combined_rows.push(combined_row);
                        matched = true;
                    }
                }
                if is_left_join && !matched {
                    let mut combined_values = left_row.values.clone();
                    for _ in 0..join_schema.columns.len() {
                        combined_values.push(Value::Null);
                    }
                    new_combined_rows.push(Row::new(combined_values));
                }
            }

            combined_schemas.push((join_alias.clone(), join_schema));
            combined_rows = new_combined_rows;
        }

        let mut final_column_offsets: HashMap<String, usize> = HashMap::new();
        let mut final_columns: Vec<ColumnDef> = Vec::new();
        let mut offset = 0;
        for (alias, schema) in &combined_schemas {
            for col in &schema.columns {
                final_column_offsets.insert(format!("{}.{}", alias, col.name), offset);
                if !final_column_offsets.contains_key(&col.name) {
                    final_column_offsets.insert(col.name.clone(), offset);
                }
                final_columns.push(col.clone());
                offset += 1;
            }
        }
        let final_schema = TableSchema {
            name: "joined".to_string(),
            table_id: 0,
            columns: final_columns,
            version: 1,
            pk_indices: vec![],
            indexes: vec![],
        };

        let mut filtered_rows = if let Some(sel) = &select.selection {
            let mut v = Vec::new();
            for row in combined_rows {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: final_column_offsets.clone(),
                    combined_row: &row,
                    combined_schema: &final_schema,
                };
                if matches!(eval_expr_join(sel, &ctx)?, Value::Boolean(true)) {
                    v.push(row);
                }
            }
            v
        } else {
            combined_rows
        };

        let group_keys_exprs = match &select.group_by {
            GroupByExpr::Expressions(exprs) => exprs,
            GroupByExpr::All => return Err(anyhow!("GROUP BY ALL not supported")),
        };
        
        let mut agg_funcs = Vec::new();
        for (i, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(Expr::Function(f)) | SelectItem::ExprWithAlias { expr: Expr::Function(f), .. } => {
                    let func_name = f.name.0.last().map(|i| i.value.to_uppercase()).unwrap_or_default();
                    if matches!(func_name.as_str(), "COUNT" | "SUM" | "AVG" | "MAX" | "MIN") {
                        agg_funcs.push((i, f.clone()));
                    }
                }
                _ => {}
            }
        }
        
        let is_agg = !group_keys_exprs.is_empty() || !agg_funcs.is_empty();

        if is_agg {
            let mut groups: HashMap<Vec<u8>, Vec<Aggregator>> = HashMap::new();
            let mut group_rows: HashMap<Vec<u8>, Row> = HashMap::new();

            for row in filtered_rows {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: final_column_offsets.clone(),
                    combined_row: &row,
                    combined_schema: &final_schema,
                };
                
                let mut key = Vec::new();
                for expr in group_keys_exprs {
                    key.push(eval_expr_join(expr, &ctx)?);
                }
                let key_bytes = bincode::serialize(&key).unwrap();

                if !groups.contains_key(&key_bytes) {
                    let mut aggs = Vec::new();
                    for (_, f) in &agg_funcs {
                        let name = f.name.0.last().unwrap().value.clone();
                        aggs.push(Aggregator::new(&name)?);
                    }
                    groups.insert(key_bytes.clone(), aggs);
                    group_rows.insert(key_bytes.clone(), row.clone());
                }

                let aggs = groups.get_mut(&key_bytes).unwrap();
                for (agg_idx, (_, f)) in agg_funcs.iter().enumerate() {
                    let arg_expr = if f.args.is_empty() { None } else {
                        match &f.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
                            _ => None
                        }
                    };
                    let val = if let Some(e) = arg_expr {
                        eval_expr_join(e, &ctx)?
                    } else {
                        Value::Int32(1)
                    };
                    aggs[agg_idx].update(&val)?;
                }
            }

            let mut final_rows = Vec::new();
            let col_names: Vec<String> = select.projection.iter().map(get_select_item_name).collect();

            for (key_bytes, aggs) in groups {
                let representative = &group_rows[&key_bytes];
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: final_column_offsets.clone(),
                    combined_row: representative,
                    combined_schema: &final_schema,
                };
                
                if let Some(having_expr) = &select.having {
                    let having_val = eval_having_expr_join(having_expr, &ctx, &agg_funcs, &aggs)?;
                    if !matches!(having_val, Value::Boolean(true)) {
                        continue;
                    }
                }
                
                let mut row_values = Vec::new();
                for (i, item) in select.projection.iter().enumerate() {
                    if let Some(agg_pos) = agg_funcs.iter().position(|(idx, _)| *idx == i) {
                        row_values.push(aggs[agg_pos].result());
                    } else {
                        let expr = match item {
                            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                            _ => return Err(anyhow!("Unsupported projection item")),
                        };
                        row_values.push(eval_expr_join(expr, &ctx)?);
                    }
                }
                final_rows.push(Row::new(row_values));
            }

            return Ok(ExecuteResult::Select { columns: col_names, rows: final_rows });
        }

        if !query.order_by.is_empty() {
            filtered_rows.sort_by(|a, b| {
                for order_expr in &query.order_by {
                    let ctx_a = JoinContext {
                        tables: HashMap::new(),
                        column_offsets: final_column_offsets.clone(),
                        combined_row: a,
                        combined_schema: &final_schema,
                    };
                    let ctx_b = JoinContext {
                        tables: HashMap::new(),
                        column_offsets: final_column_offsets.clone(),
                        combined_row: b,
                        combined_schema: &final_schema,
                    };
                    let val_a = eval_expr_join(&order_expr.expr, &ctx_a).unwrap_or(Value::Null);
                    let val_b = eval_expr_join(&order_expr.expr, &ctx_b).unwrap_or(Value::Null);
                    let cmp = super::expr::compare_values(&val_a, &val_b).unwrap_or(0);
                    if cmp != 0 {
                        let asc = order_expr.asc.unwrap_or(true);
                        return if asc {
                            if cmp > 0 { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less }
                        } else {
                            if cmp > 0 { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let mut final_rows = filtered_rows;
        if let Some(offset) = &query.offset {
            if let Ok(v) = eval_expr(&offset.value, None, None) {
                let n = match v { Value::Int64(n) => n as usize, Value::Int32(n) => n as usize, _ => 0 };
                final_rows = final_rows.into_iter().skip(n).collect();
            }
        }
        if let Some(limit) = &query.limit {
            if let Ok(v) = eval_expr(limit, None, None) {
                let n = match v { Value::Int64(n) => n as usize, Value::Int32(n) => n as usize, _ => usize::MAX };
                final_rows = final_rows.into_iter().take(n).collect();
            }
        }

        let mut cols = Vec::new();
        let mut result_rows = Vec::new();
        
        let wildcard = select.projection.iter().any(|p| matches!(p, SelectItem::Wildcard(_)));
        if wildcard {
            for (alias, schema) in &combined_schemas {
                for col in &schema.columns {
                    cols.push(format!("{}.{}", alias, col.name));
                }
            }
            result_rows = final_rows;
        } else {
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => cols.push(id.value.clone()),
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                        cols.push(parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join("."));
                    },
                    SelectItem::ExprWithAlias { alias, .. } => cols.push(alias.value.clone()),
                    SelectItem::UnnamedExpr(Expr::Function(f)) => {
                        cols.push(f.name.0.last().map(|i| i.value.clone()).unwrap_or("func".to_string()));
                    },
                    _ => cols.push("col".to_string()),
                }
            }
            
            for row in final_rows {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: final_column_offsets.clone(),
                    combined_row: &row,
                    combined_schema: &final_schema,
                };
                let mut vals = Vec::new();
                for item in &select.projection {
                    let expr = match item {
                        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                        SelectItem::Wildcard(_) => continue,
                        _ => return Err(anyhow!("Unsupported select item")),
                    };
                    vals.push(eval_expr_join(expr, &ctx)?);
                }
                result_rows.push(Row::new(vals));
            }
        }

        if select.distinct.is_some() {
            result_rows = dedup_rows(result_rows);
        }

        Ok(ExecuteResult::Select { columns: cols, rows: result_rows })
    }

    async fn execute_show_tables(&self, txn: &mut Transaction) -> Result<ExecuteResult> {
        let tables = self.store.list_tables(txn).await?;
        Ok(ExecuteResult::ShowTables { tables })
    }

    fn resolve_subqueries<'a>(&'a self, txn: &'a mut Transaction, expr: &'a Expr) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Expr>> + Send + 'a>> {
        Box::pin(async move {
            match expr {
                Expr::InSubquery { expr: inner_expr, subquery, negated } => {
                    let result = self.execute_query(txn, subquery).await?;
                    let values = match result {
                        ExecuteResult::Select { rows, .. } => {
                            rows.iter()
                                .filter_map(|row| row.values.first().cloned())
                                .map(|v| value_to_sql_expr(&v))
                                .collect::<Vec<_>>()
                        }
                        _ => return Err(anyhow!("Subquery must return a SELECT result")),
                    };
                    let resolved_inner = Box::new(self.resolve_subqueries(txn, inner_expr).await?);
                    Ok(Expr::InList {
                        expr: resolved_inner,
                        list: values,
                        negated: *negated,
                    })
                }
                Expr::BinaryOp { left, op, right } => {
                    let resolved_left = Box::new(self.resolve_subqueries(txn, left).await?);
                    let resolved_right = Box::new(self.resolve_subqueries(txn, right).await?);
                    Ok(Expr::BinaryOp {
                        left: resolved_left,
                        op: op.clone(),
                        right: resolved_right,
                    })
                }
                Expr::UnaryOp { op, expr: inner } => {
                    let resolved = Box::new(self.resolve_subqueries(txn, inner).await?);
                    Ok(Expr::UnaryOp { op: op.clone(), expr: resolved })
                }
                Expr::Nested(inner) => {
                    let resolved = Box::new(self.resolve_subqueries(txn, inner).await?);
                    Ok(Expr::Nested(resolved))
                }
                Expr::Exists { subquery, negated } => {
                    let result = self.execute_query(txn, subquery).await?;
                    let exists = match result {
                        ExecuteResult::Select { rows, .. } => !rows.is_empty(),
                        _ => false,
                    };
                    let result_bool = if *negated { !exists } else { exists };
                    Ok(Expr::Value(SqlValue::Boolean(result_bool)))
                }
                _ => Ok(expr.clone()),
            }
        })
    }

    async fn scan_and_fill(&self, txn: &mut Transaction, table_name: &str, schema: &TableSchema) -> Result<Vec<Row>> {
        let rows = self.store.scan(txn, table_name).await?;
        let mut filled_rows = Vec::with_capacity(rows.len());
        for mut row in rows {
            fill_row_defaults(&mut row, schema)?;
            filled_rows.push(row);
        }
        Ok(filled_rows)
    }

    pub fn parse_value_for_copy(&self, val: &str, data_type: &DataType) -> Value {
        let unescaped = val
            .replace("\\t", "\t")
            .replace("\\n", "\n")
            .replace("\\r", "\r")
            .replace("\\\\", "\\");

        match data_type {
            DataType::Boolean => {
                match unescaped.to_lowercase().as_str() {
                    "t" | "true" | "1" | "yes" | "on" => Value::Boolean(true),
                    "f" | "false" | "0" | "no" | "off" => Value::Boolean(false),
                    _ => Value::Text(unescaped),
                }
            }
            DataType::Int32 => {
                unescaped.parse::<i32>().map(Value::Int32).unwrap_or(Value::Text(unescaped))
            }
            DataType::Int64 => {
                unescaped.parse::<i64>().map(Value::Int64).unwrap_or(Value::Text(unescaped))
            }
            DataType::Float64 => {
                unescaped.parse::<f64>().map(Value::Float64).unwrap_or(Value::Text(unescaped))
            }
            DataType::Timestamp => {
                if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(&unescaped, "%Y-%m-%d %H:%M:%S%.f") {
                    Value::Timestamp(ts.and_utc().timestamp_millis())
                } else if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(&unescaped, "%Y-%m-%d %H:%M:%S") {
                    Value::Timestamp(ts.and_utc().timestamp_millis())
                } else if let Ok(d) = chrono::NaiveDate::parse_from_str(&unescaped, "%Y-%m-%d") {
                    Value::Timestamp(d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis())
                } else {
                    Value::Text(unescaped)
                }
            }
            DataType::Uuid => {
                if let Ok(u) = uuid::Uuid::parse_str(&unescaped) {
                    Value::Uuid(*u.as_bytes())
                } else {
                    Value::Text(unescaped)
                }
            }
            DataType::Bytes => {
                if unescaped.starts_with("\\x") {
                    hex::decode(&unescaped[2..]).map(Value::Bytes).unwrap_or(Value::Bytes(unescaped.into_bytes()))
                } else {
                    Value::Bytes(unescaped.into_bytes())
                }
            }
            DataType::Text | DataType::Interval => Value::Text(unescaped),
        }
    }

    pub async fn execute_copy_insert(&self, session: &mut Session, table_name: &str, col_values: Vec<(String, Value)>) -> Result<()> {
        let is_autocommit = !session.is_in_transaction();
        
        if is_autocommit {
            session.begin().await?;
        }

        let result = async {
            let txn = session.get_mut_txn().expect("Transaction must be active");
            let schema = self.store.get_schema(txn, table_name).await?
                .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;

            let mut row_values = vec![Value::Null; schema.columns.len()];
            
            for (col_name, value) in col_values {
                if let Some(idx) = schema.column_index(&col_name) {
                    row_values[idx] = value;
                }
            }

            for (i, col) in schema.columns.iter().enumerate() {
                if matches!(row_values[i], Value::Null) {
                    if col.is_serial {
                        let next_id = self.store.next_sequence_value(txn, schema.table_id).await?;
                        row_values[i] = Value::Int32(next_id);
                    } else if let Some(ref default_expr) = col.default_expr {
                        row_values[i] = eval_default_expr(default_expr)?;
                    }
                }
            }

            let mut row = Row { values: row_values };
            fill_row_defaults(&mut row, &schema)?;

            self.store.insert(txn, &schema.name, row.clone()).await?;

            let pk_values = schema.get_pk_values(&row);
            for index in &schema.indexes {
                let idx_values = schema.get_index_values(index, &row);
                self.store.create_index_entry(txn, schema.table_id, index.id, &idx_values, &pk_values, index.unique).await?;
            }

            Ok::<(), anyhow::Error>(())
        }.await;

        if is_autocommit {
            if result.is_ok() {
                session.commit().await?;
            } else {
                session.rollback().await?;
            }
        }
        
        result
    }
}

fn dedup_rows(rows: Vec<Row>) -> Vec<Row> {
    use std::collections::HashSet;
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut result = Vec::new();
    for row in rows {
        let key = bincode::serialize(&row.values).unwrap_or_default();
        if seen.insert(key) {
            result.push(row);
        }
    }
    result
}

fn value_to_sql_expr(v: &Value) -> Expr {
    match v {
        Value::Null => Expr::Value(SqlValue::Null),
        Value::Boolean(b) => Expr::Value(SqlValue::Boolean(*b)),
        Value::Int32(i) => Expr::Value(SqlValue::Number(i.to_string(), false)),
        Value::Int64(i) => Expr::Value(SqlValue::Number(i.to_string(), false)),
        Value::Float64(f) => Expr::Value(SqlValue::Number(f.to_string(), false)),
        Value::Text(s) => Expr::Value(SqlValue::SingleQuotedString(s.clone())),
        Value::Bytes(b) => Expr::Value(SqlValue::SingleQuotedString(format!("\\x{}", hex::encode(b)))),
        Value::Timestamp(ts) => Expr::Value(SqlValue::Number(ts.to_string(), false)),
        Value::Interval(ms) => Expr::Value(SqlValue::Number(ms.to_string(), false)),
        Value::Uuid(bytes) => {
            let uuid = uuid::Uuid::from_bytes(*bytes);
            Expr::Value(SqlValue::SingleQuotedString(uuid.to_string()))
        }
    }
}

fn get_select_item_name(item: &SelectItem) -> String {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::UnnamedExpr(expr) => get_expr_name(expr),
        SelectItem::Wildcard(_) => "*".to_string(),
        _ => "?column?".to_string(),
    }
}

fn get_expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()).unwrap_or_else(|| "?column?".to_string()),
        Expr::Function(f) => f.name.to_string().to_lowercase(),
        _ => "?column?".to_string(),
    }
}

fn fill_row_defaults(row: &mut Row, schema: &TableSchema) -> Result<()> {
    if row.values.len() < schema.columns.len() {
        for i in row.values.len()..schema.columns.len() {
            let col = &schema.columns[i];
            let val = if let Some(expr_str) = &col.default_expr {
                eval_default_expr(expr_str)?
            } else { Value::Null };
            row.values.push(val);
        }
    }
    Ok(())
}

fn eval_default_expr(expr_str: &str) -> Result<Value> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    
    let sql = format!("SELECT {}", expr_str);
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| anyhow!("Failed to parse default expr: {}", e))?;
    
    if let Some(sqlparser::ast::Statement::Query(q)) = ast.into_iter().next() {
        if let sqlparser::ast::SetExpr::Select(s) = *q.body {
            if let Some(sqlparser::ast::SelectItem::UnnamedExpr(e)) = s.projection.into_iter().next() {
                return eval_expr(&e, None, None);
            }
        }
    }
    Ok(Value::Text(expr_str.to_string()))
}

fn is_serial_type(sql_type: &SqlDataType) -> bool {
    match sql_type {
        SqlDataType::Custom(name, _) => {
             if let Some(ident) = name.0.last() { ident.value.eq_ignore_ascii_case("SERIAL") } else { false }
        }
        _ => false,
    }
}

fn convert_data_type(sql_type: &SqlDataType) -> Result<DataType> {
    match sql_type {
        SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::SmallInt(_) | SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int32),
        SqlDataType::BigInt(_) => Ok(DataType::Int64),
        SqlDataType::Float(_) | SqlDataType::Double | SqlDataType::Real | SqlDataType::Numeric(_) | SqlDataType::Decimal(_) => Ok(DataType::Float64),
        SqlDataType::Varchar(_) | SqlDataType::Text | SqlDataType::String(_) | SqlDataType::Char(_) | SqlDataType::Character(_) | SqlDataType::CharacterVarying(_) => Ok(DataType::Text),
        SqlDataType::Bytea => Ok(DataType::Bytes),
        SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp),
        SqlDataType::Date => Ok(DataType::Timestamp),
        SqlDataType::Uuid => Ok(DataType::Uuid),
        SqlDataType::Custom(name, _) => {
            if let Some(ident) = name.0.last() {
                let type_name = ident.value.to_uppercase();
                match type_name.as_str() {
                    "SERIAL" => Ok(DataType::Int32),
                    "BIGSERIAL" => Ok(DataType::Int64),
                    _ => Ok(DataType::Text)
                }
            } else {
                Ok(DataType::Text)
            }
        }
        SqlDataType::Array(inner) => {
            match inner {
                sqlparser::ast::ArrayElemTypeDef::AngleBracket(inner_type) => convert_data_type(inner_type),
                sqlparser::ast::ArrayElemTypeDef::SquareBracket(inner_type) => convert_data_type(inner_type),
                _ => Ok(DataType::Text),
            }
        }
        _ => Err(anyhow!("Unsupported data type: {:?}", sql_type)),
    }
}

fn extract_eq_conditions(expr: &Expr, index_cols: &[String]) -> Option<Vec<Value>> {
    let mut values = vec![None; index_cols.len()];
    extract_conditions_recursive(expr, index_cols, &mut values);

    if values.iter().all(|v| v.is_some()) {
        Some(values.into_iter().map(|v| v.unwrap()).collect())
    } else {
        None
    }
}

fn extract_conditions_recursive(expr: &Expr, index_cols: &[String], values: &mut Vec<Option<Value>>) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    extract_conditions_recursive(left, index_cols, values);
                    extract_conditions_recursive(right, index_cols, values);
                }
                BinaryOperator::Eq => {
                    if let Expr::Identifier(ident) = &**left {
                        if let Some(idx) = index_cols.iter().position(|c| c == &ident.value) {
                            if let Ok(val) = eval_expr(right, None, None) {
                                values[idx] = Some(val);
                            }
                        }
                    } else if let Expr::Identifier(ident) = &**right {
                        if let Some(idx) = index_cols.iter().position(|c| c == &ident.value) {
                            if let Ok(val) = eval_expr(left, None, None) {
                                values[idx] = Some(val);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Expr::Nested(e) => extract_conditions_recursive(e, index_cols, values),
        _ => {}
    }
}

fn eval_having_expr(
    expr: &Expr,
    row: &Row,
    schema: &TableSchema,
    agg_funcs: &[(usize, sqlparser::ast::Function)],
    aggs: &[Aggregator],
) -> Result<Value> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_having_expr(left, row, schema, agg_funcs, aggs)?;
            let right_val = eval_having_expr(right, row, schema, agg_funcs, aggs)?;
            super::expr::eval_binary_op_public(left_val, op, right_val)
        }
        Expr::Function(f) => {
            let func_name = f.name.0.last().map(|i| i.value.to_uppercase()).unwrap_or_default();
            for (i, (_, agg_f)) in agg_funcs.iter().enumerate() {
                let agg_name = agg_f.name.0.last().map(|n| n.value.to_uppercase()).unwrap_or_default();
                if agg_name == func_name && args_match(f, agg_f) {
                    return Ok(aggs[i].result());
                }
            }
            let mut temp_agg = Aggregator::new(&func_name)?;
            let arg_expr = if f.args.is_empty() { None } else {
                match &f.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None
                }
            };
            if let Some(e) = arg_expr {
                let val = eval_expr(e, Some(row), Some(schema))?;
                temp_agg.update(&val)?;
            } else {
                temp_agg.update(&Value::Int32(1))?;
            }
            Ok(temp_agg.result())
        }
        Expr::Nested(e) => eval_having_expr(e, row, schema, agg_funcs, aggs),
        Expr::Value(v) => super::expr::eval_value_public(v),
        _ => eval_expr(expr, Some(row), Some(schema)),
    }
}

fn args_match(f1: &sqlparser::ast::Function, f2: &sqlparser::ast::Function) -> bool {
    if f1.args.len() != f2.args.len() {
        return false;
    }
    for (a1, a2) in f1.args.iter().zip(f2.args.iter()) {
        match (a1, a2) {
            (FunctionArg::Unnamed(FunctionArgExpr::Wildcard), FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => {}
            (FunctionArg::Unnamed(FunctionArgExpr::Expr(e1)), FunctionArg::Unnamed(FunctionArgExpr::Expr(e2))) => {
                if format!("{:?}", e1) != format!("{:?}", e2) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

fn eval_having_expr_join(
    expr: &Expr,
    ctx: &JoinContext,
    agg_funcs: &[(usize, sqlparser::ast::Function)],
    aggs: &[Aggregator],
) -> Result<Value> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_having_expr_join(left, ctx, agg_funcs, aggs)?;
            let right_val = eval_having_expr_join(right, ctx, agg_funcs, aggs)?;
            super::expr::eval_binary_op_public(left_val, op, right_val)
        }
        Expr::Function(f) => {
            let func_name = f.name.0.last().map(|i| i.value.to_uppercase()).unwrap_or_default();
            for (i, (_, agg_f)) in agg_funcs.iter().enumerate() {
                let agg_name = agg_f.name.0.last().map(|n| n.value.to_uppercase()).unwrap_or_default();
                if agg_name == func_name && args_match(f, agg_f) {
                    return Ok(aggs[i].result());
                }
            }
            let mut temp_agg = Aggregator::new(&func_name)?;
            let arg_expr = if f.args.is_empty() { None } else {
                match &f.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None
                }
            };
            if let Some(e) = arg_expr {
                let val = eval_expr_join(e, ctx)?;
                temp_agg.update(&val)?;
            } else {
                temp_agg.update(&Value::Int32(1))?;
            }
            Ok(temp_agg.result())
        }
        Expr::Nested(e) => eval_having_expr_join(e, ctx, agg_funcs, aggs),
        Expr::Value(v) => super::expr::eval_value_public(v),
        _ => eval_expr_join(expr, ctx),
    }
}

fn get_skip_reason(sql_upper: &str) -> Option<String> {
    if sql_upper.starts_with("DROP DATABASE") { return Some("DROP DATABASE not supported".into()); }
    if sql_upper.starts_with("CREATE DATABASE") { return Some("CREATE DATABASE not supported".into()); }
    if sql_upper.starts_with("ALTER DATABASE") { return Some("ALTER DATABASE not supported".into()); }
    if sql_upper.starts_with("\\") { return Some("psql meta-command not supported".into()); }
    if sql_upper.starts_with("COPY ") || sql_upper.contains(" FROM STDIN") { return Some("COPY not supported".into()); }
    None
}

fn get_unsupported_reason(sql_upper: &str) -> Option<String> {
    if sql_upper.starts_with("CREATE TRIGGER") { return Some("CREATE TRIGGER not supported".into()); }
    if sql_upper.starts_with("CREATE DOMAIN") { return Some("CREATE DOMAIN not supported".into()); }
    if sql_upper.starts_with("CREATE AGGREGATE") { return Some("CREATE AGGREGATE not supported".into()); }
    if sql_upper.starts_with("ALTER TYPE") { return Some("ALTER TYPE not supported".into()); }
    if sql_upper.starts_with("ALTER DOMAIN") { return Some("ALTER DOMAIN not supported".into()); }
    if sql_upper.starts_with("ALTER AGGREGATE") { return Some("ALTER AGGREGATE not supported".into()); }
    if sql_upper.starts_with("ALTER FUNCTION") { return Some("ALTER FUNCTION not supported".into()); }
    if sql_upper.starts_with("ALTER SEQUENCE") { return Some("ALTER SEQUENCE not supported".into()); }
    if sql_upper.starts_with("ALTER TABLE") && sql_upper.contains("OWNER TO") { return Some("ALTER TABLE OWNER TO not supported".into()); }
    if sql_upper.starts_with("CREATE TYPE") && sql_upper.contains("AS ENUM") { return Some("CREATE TYPE AS ENUM not supported".into()); }
    if sql_upper.starts_with("CREATE TYPE") && sql_upper.contains("AS (") { return Some("CREATE TYPE AS composite not supported".into()); }
    if sql_upper.contains("$_$") || sql_upper.contains("$$") { return Some("Dollar-quoted strings not supported".into()); }
    if sql_upper.starts_with("CREATE SEQUENCE") && sql_upper.contains("INCREMENT") { return Some("CREATE SEQUENCE not supported".into()); }
    if sql_upper.starts_with("CREATE INDEX") && sql_upper.contains("USING GIST") { return Some("GIST index not supported".into()); }
    None
}
