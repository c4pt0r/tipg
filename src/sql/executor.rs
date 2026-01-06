//! SQL executor

use super::{parse_sql, ExecuteResult, expr::{eval_expr, eval_expr_join, JoinContext}, Session, Aggregator};
use crate::auth::{AuthManager, User, Role, Privilege, PrivilegeObject};
use crate::storage::TikvStore;
use crate::types::{ColumnDef, DataType, Row, TableSchema, Value, IndexDef};
use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AlterTableOperation, AlterRoleOperation, Assignment, ColumnDef as SqlColumnDef, ColumnOption, 
    DataType as SqlDataType, Expr, Ident, ObjectName, Query, SelectItem, SetExpr, Statement, 
    TableConstraint, Values, FunctionArg, FunctionArgExpr, OrderByExpr, BinaryOperator, GroupByExpr, 
    JoinOperator, JoinConstraint, TableFactor, LockType, Password as SqlPassword,
    Value as SqlValue, Cte, WindowType, SetOperator, SetQuantifier, OnInsert, OnConflictAction,
    GrantObjects, Privileges,
};
use std::sync::Arc;
use std::collections::HashMap;
use tikv_client::Transaction;
use tracing::debug;

pub struct Executor {
    store: Arc<TikvStore>,
    auth_manager: AuthManager,
}

impl Executor {
    pub fn new(store: Arc<TikvStore>) -> Self {
        Self { 
            store,
            auth_manager: AuthManager::new(None),
        }
    }

    pub fn new_with_namespace(store: Arc<TikvStore>, namespace: Option<String>) -> Self {
        Self {
            store,
            auth_manager: AuthManager::new(namespace),
        }
    }

    pub fn store(&self) -> Arc<TikvStore> {
        self.store.clone()
    }

    pub fn auth_manager(&self) -> &AuthManager {
        &self.auth_manager
    }

    /// Execute a SQL statement string using the provided session
    /// Supports multiple statements separated by semicolons (e.g., "BEGIN; UPDATE...; COMMIT;")
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

        // Execute all statements in order, returning the result of the last one
        let mut last_result = ExecuteResult::Empty;
        
        for stmt in &statements {
            debug!("Executing statement: {:?}", stmt);

            last_result = match stmt {
                // Transaction Control
                Statement::StartTransaction { .. } => {
                    session.begin().await?;
                    ExecuteResult::Empty
                },
                Statement::Commit { .. } => {
                    session.commit().await?;
                    ExecuteResult::Empty
                },
                Statement::Rollback { .. } => {
                    session.rollback().await?;
                    ExecuteResult::Empty
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
                    
                    res?
                }
            };
        }
        
        Ok(last_result)
    }

    /// Execute a parsed SQL statement on a given transaction
    async fn execute_statement_on_txn(&self, txn: &mut Transaction, stmt: &Statement) -> Result<ExecuteResult> {
        match stmt {
            Statement::CreateTable { name, columns, constraints, if_not_exists, query, temporary, .. } => {
                if let Some(q) = query {
                    self.execute_create_table_as(txn, name, q, columns, *if_not_exists, *temporary).await
                } else {
                    self.execute_create_table(txn, name, columns, constraints, *if_not_exists).await
                }
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
                    ObjectType::View => self.execute_drop_view(txn, names, *if_exists).await,
                    ObjectType::Index => self.execute_drop_index(txn, names, *if_exists).await,
                    ObjectType::Role => self.execute_drop_role(txn, names, *if_exists).await,
                    ObjectType::Sequence => Ok(ExecuteResult::Empty),
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
            Statement::Insert { table_name, columns, source, returning, on, .. } => {
                self.execute_insert(txn, table_name, columns, source, returning, on).await
            }
            Statement::Delete { from, selection, returning, .. } => {
                self.execute_delete(txn, from, selection, returning).await
            }
            Statement::Update { table, assignments, from, selection, returning, .. } => {
                self.execute_update(txn, table, assignments, from, selection, returning).await
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
            Statement::CreateView { name, query, or_replace, .. } => {
                self.execute_create_view(txn, name, query, *or_replace).await
            }
            Statement::AlterIndex { .. } => {
                Ok(ExecuteResult::Empty)
            }
            Statement::CreateRole { names, if_not_exists, login, inherit, password, superuser, create_db, create_role, connection_limit, valid_until, .. } => {
                self.execute_create_role(txn, names, *if_not_exists, login, inherit, password, superuser, create_db, create_role, connection_limit, valid_until).await
            }
            Statement::AlterRole { name, operation } => {
                self.execute_alter_role(txn, name, operation).await
            }
            Statement::Grant { privileges, objects, grantees, with_grant_option, .. } => {
                self.execute_grant(txn, privileges, &Some(objects.clone()), grantees, *with_grant_option).await
            }
            Statement::Revoke { privileges, objects, grantees, .. } => {
                self.execute_revoke(txn, privileges, &Some(objects.clone()), grantees).await
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

    async fn execute_create_table_as(&self, txn: &mut Transaction, name: &ObjectName, query: &Query, columns: &[SqlColumnDef], if_not_exists: bool, _temporary: bool) -> Result<ExecuteResult> {
        let table_name = name.0.last().ok_or_else(|| anyhow!("Invalid table name"))?.value.clone();
        if if_not_exists && self.store.table_exists(txn, &table_name).await? {
            return Ok(ExecuteResult::CreateTable { table_name });
        }
        
        let ctes = self.build_cte_context(txn, query).await?;
        let result = self.execute_query_with_ctes(txn, query, &ctes).await?;
        
        let (result_cols, result_rows) = match result {
            ExecuteResult::Select { columns: cols, rows } => (cols, rows),
            _ => return Err(anyhow!("CREATE TABLE AS requires a SELECT query")),
        };
        
        let col_defs: Vec<ColumnDef> = if columns.is_empty() {
            result_cols.iter().enumerate().map(|(i, col_name)| {
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
            }).collect()
        } else {
            columns.iter().map(|col| {
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
            }).collect()
        };
        
        let table_id = self.store.next_table_id(txn).await?;
        let schema = TableSchema {
            name: table_name.clone(),
            table_id,
            columns: col_defs,
            version: 1,
            pk_indices: vec![],
            indexes: vec![],
        };
        self.store.create_table(txn, schema.clone()).await?;
        
        for row in result_rows {
            self.store.upsert(txn, &table_name, row).await?;
        }
        
        Ok(ExecuteResult::CreateTable { table_name })
    }

    async fn create_table_from_result(&self, txn: &mut Transaction, target_name: &ObjectName, result: ExecuteResult) -> Result<ExecuteResult> {
        let table_name = target_name.0.last().ok_or_else(|| anyhow!("Invalid table name"))?.value.clone();
        
        if self.store.table_exists(txn, &table_name).await? {
            return Err(anyhow!("relation \"{}\" already exists", table_name));
        }
        
        let (result_cols, result_rows) = match result {
            ExecuteResult::Select { columns: cols, rows } => (cols, rows),
            _ => return Err(anyhow!("SELECT INTO requires a SELECT query")),
        };
        
        let col_defs: Vec<ColumnDef> = result_cols.iter().enumerate().map(|(i, col_name)| {
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
        }).collect();
        
        let table_id = self.store.next_table_id(txn).await?;
        let schema = TableSchema {
            name: table_name.clone(),
            table_id,
            columns: col_defs,
            version: 1,
            pk_indices: vec![],
            indexes: vec![],
        };
        self.store.create_table(txn, schema).await?;
        
        let row_count = result_rows.len();
        for row in result_rows {
            self.store.upsert(txn, &table_name, row).await?;
        }
        
        Ok(ExecuteResult::Insert { affected_rows: row_count as u64 })
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

    async fn execute_create_view(&self, txn: &mut Transaction, name: &ObjectName, query: &Query, or_replace: bool) -> Result<ExecuteResult> {
        let view_name = name.0.last().ok_or_else(|| anyhow!("Invalid view name"))?.value.clone();
        let view_name_lower = view_name.to_lowercase();
        
        if self.store.get_view(txn, &view_name_lower).await?.is_some() {
            if or_replace {
                self.store.drop_view(txn, &view_name_lower).await?;
            } else {
                return Err(anyhow!("View '{}' already exists", view_name));
            }
        }
        
        let query_str = query.to_string();
        self.store.create_view(txn, &view_name_lower, &query_str).await?;
        Ok(ExecuteResult::CreateView { view_name })
    }

    async fn execute_drop_view(&self, txn: &mut Transaction, names: &[ObjectName], if_exists: bool) -> Result<ExecuteResult> {
        let mut last = String::new();
        for name in names {
            let v = name.0.last().unwrap().value.to_lowercase();
            if !self.store.drop_view(txn, &v).await? && !if_exists {
                return Err(anyhow!("View '{}' does not exist", v));
            }
            last = v;
        }
        Ok(ExecuteResult::DropView { view_name: last })
    }

    async fn execute_drop_index(&self, txn: &mut Transaction, names: &[ObjectName], if_exists: bool) -> Result<ExecuteResult> {
        let mut last_index = String::new();
        for name in names {
            let idx_name = name.0.last().unwrap().value.clone();
            let mut found = false;
            
            let tables = self.store.list_tables(txn).await?;
            for table_name in tables {
                let mut schema = match self.store.get_schema(txn, &table_name).await? {
                    Some(s) => s,
                    None => continue,
                };
                
                if let Some(pos) = schema.indexes.iter().position(|i| i.name == idx_name) {
                    let index = schema.indexes.remove(pos);
                    let rows = self.scan_and_fill(txn, &table_name, &schema).await?;
                    for row in rows {
                        let idx_values = schema.get_index_values(&index, &row);
                        let pk_values = schema.get_pk_values(&row);
                        self.store.delete_index_entry(txn, schema.table_id, index.id, &idx_values, &pk_values, index.unique).await?;
                    }
                    self.store.update_schema(txn, schema).await?;
                    found = true;
                    last_index = idx_name.clone();
                    break;
                }
            }
            
            if !found && !if_exists {
                return Err(anyhow!("Index '{}' does not exist", idx_name));
            }
        }
        Ok(ExecuteResult::DropIndex { index_name: last_index })
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
            AlterTableOperation::DropColumn { column_name, if_exists, .. } => {
                let col_name = column_name.value.clone();
                let col_idx = schema.column_index(&col_name);
                match col_idx {
                    Some(idx) => {
                        if schema.pk_indices.contains(&idx) {
                            return Err(anyhow!("Cannot drop primary key column '{}'", col_name));
                        }
                        for index in &schema.indexes {
                            if index.columns.contains(&col_name) {
                                return Err(anyhow!("Cannot drop column '{}' used in index '{}'", col_name, index.name));
                            }
                        }
                        let rows = self.scan_and_fill(txn, &t, &schema).await?;
                        for row in rows {
                            let pk_values = schema.get_pk_values(&row);
                            let mut new_values = row.values.clone();
                            new_values.remove(idx);
                            let new_row = Row::new(new_values);
                            self.store.upsert(txn, &t, new_row).await?;
                        }
                        schema.columns.remove(idx);
                        for pk_idx in &mut schema.pk_indices {
                            if *pk_idx > idx { *pk_idx -= 1; }
                        }
                        schema.version += 1;
                        self.store.update_schema(txn, schema).await?;
                    }
                    None => {
                        if !if_exists {
                            return Err(anyhow!("Column '{}' does not exist", col_name));
                        }
                    }
                }
            },
            AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                let old_name = old_column_name.value.clone();
                let new_name = new_column_name.value.clone();
                let col_idx = schema.column_index(&old_name).ok_or_else(|| anyhow!("Column '{}' does not exist", old_name))?;
                if schema.column_index(&new_name).is_some() {
                    return Err(anyhow!("Column '{}' already exists", new_name));
                }
                schema.columns[col_idx].name = new_name.clone();
                for index in &mut schema.indexes {
                    for col in &mut index.columns {
                        if *col == old_name { *col = new_name.clone(); }
                    }
                }
                schema.version += 1;
                self.store.update_schema(txn, schema).await?;
            },
            AlterTableOperation::RenameTable { .. } => {},
            _ => return Err(anyhow!("Unsupported ALTER")),
        }
        Ok(ExecuteResult::AlterTable { table_name: t })
    }

    async fn execute_insert(&self, txn: &mut Transaction, table_name: &ObjectName, columns: &[Ident], source: &Option<Box<Query>>, returning: &Option<Vec<SelectItem>>, on_conflict: &Option<OnInsert>) -> Result<ExecuteResult> {
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
            for (i, c) in schema.columns.iter().enumerate() {
                row_vals[i] = coerce_value_for_column(row_vals[i].clone(), c)?;
            }
            let row = Row::new(row_vals);
            let pk_values = schema.get_pk_values(&row);
            
            let insert_result = self.store.insert(txn, &t, row.clone()).await;
            let final_row = match insert_result {
                Ok(()) => {
                    for index in &schema.indexes {
                        let idx_values = schema.get_index_values(index, &row);
                        self.store.create_index_entry(txn, schema.table_id, index.id, &idx_values, &pk_values, index.unique).await?;
                    }
                    row
                }
                Err(e) if e.to_string().contains("Duplicate primary key") => {
                    match on_conflict {
                        Some(OnInsert::OnConflict(oc)) => {
                            match &oc.action {
                                OnConflictAction::DoNothing => continue,
                                OnConflictAction::DoUpdate(do_update) => {
                                    let existing_rows = self.store.batch_get_rows(txn, schema.table_id, vec![pk_values.clone()], &schema).await?;
                                    if existing_rows.is_empty() {
                                        return Err(anyhow!("Failed to fetch existing row for upsert"));
                                    }
                                    let existing_row = &existing_rows[0];
                                    let mut updated_vals = existing_row.values.clone();
                                    for assignment in &do_update.assignments {
                                        let col_name = assignment.id.last().unwrap().value.clone();
                                        let col_idx = schema.column_index(&col_name).ok_or_else(|| anyhow!("Unknown column in DO UPDATE: {}", col_name))?;
                                        updated_vals[col_idx] = eval_expr(&assignment.value, Some(existing_row), Some(&schema))?;
                                    }
                                    let updated_row = Row::new(updated_vals);
                                    for index in &schema.indexes {
                                        let old_idx = schema.get_index_values(index, existing_row);
                                        self.store.delete_index_entry(txn, schema.table_id, index.id, &old_idx, &pk_values, index.unique).await?;
                                    }
                                    self.store.upsert(txn, &t, updated_row.clone()).await?;
                                    for index in &schema.indexes {
                                        let new_idx = schema.get_index_values(index, &updated_row);
                                        self.store.create_index_entry(txn, schema.table_id, index.id, &new_idx, &pk_values, index.unique).await?;
                                    }
                                    updated_row
                                }
                            }
                        }
                        Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
                            let existing_rows = self.store.batch_get_rows(txn, schema.table_id, vec![pk_values.clone()], &schema).await?;
                            if existing_rows.is_empty() {
                                return Err(anyhow!("Failed to fetch existing row for upsert"));
                            }
                            let existing_row = &existing_rows[0];
                            let mut updated_vals = existing_row.values.clone();
                            for assignment in assignments {
                                let col_name = assignment.id.last().unwrap().value.clone();
                                let col_idx = schema.column_index(&col_name).ok_or_else(|| anyhow!("Unknown column: {}", col_name))?;
                                updated_vals[col_idx] = eval_expr(&assignment.value, Some(existing_row), Some(&schema))?;
                            }
                            let updated_row = Row::new(updated_vals);
                            for index in &schema.indexes {
                                let old_idx = schema.get_index_values(index, existing_row);
                                self.store.delete_index_entry(txn, schema.table_id, index.id, &old_idx, &pk_values, index.unique).await?;
                            }
                            self.store.upsert(txn, &t, updated_row.clone()).await?;
                            for index in &schema.indexes {
                                let new_idx = schema.get_index_values(index, &updated_row);
                                self.store.create_index_entry(txn, schema.table_id, index.id, &new_idx, &pk_values, index.unique).await?;
                            }
                            updated_row
                        }
                        None => return Err(e),
                        _ => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            };
            
            affected += 1;
            if let Some(items) = returning {
                let mut vals = Vec::new();
                for item in items {
                    match item {
                        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => vals.push(eval_expr(e, Some(&final_row), Some(&schema))?),
                        SelectItem::Wildcard(_) => vals.extend(final_row.values.clone()),
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

    async fn execute_update(&self, txn: &mut Transaction, table: &sqlparser::ast::TableWithJoins, assignments: &[Assignment], from: &Option<sqlparser::ast::TableWithJoins>, selection: &Option<Expr>, returning: &Option<Vec<SelectItem>>) -> Result<ExecuteResult> {
        let t = match &table.relation { sqlparser::ast::TableFactor::Table { name, .. } => name.0.last().unwrap().value.clone(), _ => return Err(anyhow!("Unsupported")) };
        let table_alias = match &table.relation {
            sqlparser::ast::TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| t.clone()),
            _ => t.clone(),
        };
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
        
        let (from_schema, from_rows, from_alias) = if let Some(from_table) = from {
            let from_name = match &from_table.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => name.0.last().unwrap().value.clone(),
                _ => return Err(anyhow!("Unsupported FROM table")),
            };
            let from_alias_str = match &from_table.relation {
                sqlparser::ast::TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| from_name.clone()),
                _ => from_name.clone(),
            };
            let fs = self.store.get_schema(txn, &from_name).await?.ok_or_else(|| anyhow!("FROM table not found"))?;
            let fr = self.scan_and_fill(txn, &from_name, &fs).await?;
            (Some(fs), Some(fr), Some(from_alias_str))
        } else {
            (None, None, None)
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
        
        for r in &rows {
            let matching_from_rows: Vec<&Row> = if let (Some(ref fs), Some(ref fr), Some(ref fa)) = (&from_schema, &from_rows, &from_alias) {
                let mut combined_col_defs: Vec<ColumnDef> = schema.columns.clone();
                combined_col_defs.extend(fs.columns.clone());
                let combined_schema = TableSchema {
                    name: "joined".to_string(),
                    table_id: 0,
                    columns: combined_col_defs,
                    version: 1,
                    pk_indices: vec![],
                    indexes: vec![],
                };
                
                let mut column_offsets: HashMap<String, usize> = HashMap::new();
                for (i, col) in schema.columns.iter().enumerate() {
                    column_offsets.insert(format!("{}.{}", table_alias, col.name), i);
                    column_offsets.insert(col.name.clone(), i);
                }
                let offset = schema.columns.len();
                for (i, col) in fs.columns.iter().enumerate() {
                    column_offsets.insert(format!("{}.{}", fa, col.name), offset + i);
                    if !column_offsets.contains_key(&col.name) {
                        column_offsets.insert(col.name.clone(), offset + i);
                    }
                }
                
                fr.iter().filter(|from_row| {
                    if let Some(ref sel) = resolved_selection {
                        let mut combined_values = r.values.clone();
                        combined_values.extend(from_row.values.clone());
                        let combined_row = Row::new(combined_values);
                        let ctx = JoinContext {
                            tables: HashMap::new(),
                            column_offsets: column_offsets.clone(),
                            combined_row: &combined_row,
                            combined_schema: &combined_schema,
                        };
                        matches!(eval_expr_join(sel, &ctx), Ok(Value::Boolean(true)))
                    } else {
                        true
                    }
                }).collect()
            } else {
                if let Some(ref e) = resolved_selection {
                    if !matches!(eval_expr(e, Some(r), Some(&schema)), Ok(Value::Boolean(true))) {
                        continue;
                    }
                }
                vec![r]
            };
            
            if matching_from_rows.is_empty() && from.is_some() {
                continue;
            }
            
            let eval_row = if let (Some(ref fs), Some(ref fa)) = (&from_schema, &from_alias) {
                if let Some(first_from) = matching_from_rows.first() {
                    let mut combined_col_defs: Vec<ColumnDef> = schema.columns.clone();
                    combined_col_defs.extend(fs.columns.clone());
                    let combined_schema = TableSchema {
                        name: "joined".to_string(),
                        table_id: 0,
                        columns: combined_col_defs,
                        version: 1,
                        pk_indices: vec![],
                        indexes: vec![],
                    };
                    let mut combined_values = r.values.clone();
                    combined_values.extend(first_from.values.clone());
                    Some((Row::new(combined_values), combined_schema, fa.clone()))
                } else {
                    None
                }
            } else {
                None
            };
            
            let mut vals = r.values.clone();
            for (i, a) in assignments.iter().enumerate() {
                let raw_val = if let Some((ref combined_row, ref combined_schema, ref _fa)) = eval_row {
                    eval_expr(&a.value, Some(combined_row), Some(combined_schema))?
                } else {
                    eval_expr(&a.value, Some(r), Some(&schema))?
                };
                vals[indices[i]] = coerce_value_for_column(raw_val, &schema.columns[indices[i]])?;
            }
            let new_row = Row::new(vals);
            let pks = schema.get_pk_values(r);
            for index in &schema.indexes {
                let old_idx = schema.get_index_values(index, r);
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
        // Handle UNION/INTERSECT/EXCEPT
        if let SetExpr::SetOperation { op, set_quantifier, left, right } = &*query.body {
            return self.execute_set_operation(txn, op, set_quantifier, left, right, ctes).await;
        }
        
        let select = match &*query.body { SetExpr::Select(s) => s, _ => return Err(anyhow!("Only SELECT supported")) };
        
        let select_into_target = select.into.as_ref().map(|into| (into.name.clone(), into.temporary));
        
        if select.from.is_empty() {
            let result = self.execute_tableless_query(txn, select).await?;
            if let Some((target_name, _temp)) = select_into_target {
                return self.create_table_from_result(txn, &target_name, result).await;
            }
            return Ok(result);
        }
        
        let has_joins = !select.from[0].joins.is_empty();
        
        if has_joins {
            let result = self.execute_join_query_with_ctes(txn, query, select, ctes).await?;
            if let Some((target_name, _temp)) = select_into_target {
                return self.create_table_from_result(txn, &target_name, result).await;
            }
            return Ok(result);
        }
        
        let t = match &select.from[0].relation { TableFactor::Table { name, .. } => name.0.last().unwrap().value.clone(), _ => return Err(anyhow!("Unsupported table")) };
        let t_lower = t.to_lowercase();
        
        let (schema, all_rows_base) = self.get_table_data(txn, &t, ctes).await?;
        let is_virtual = ctes.contains_key(&t_lower) || self.store.get_view(txn, &t_lower).await?.is_some();
        
        let resolved_selection = if let Some(sel) = &select.selection {
            Some(self.resolve_subqueries(txn, sel).await?)
        } else {
            None
        };
        
        let resolved_projection = self.resolve_projection_subqueries(txn, &select.projection).await?;
        
        let all_rows = if is_virtual {
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
        
        let window_funcs = extract_window_functions(&select.projection);
        
        let mut agg_funcs = Vec::new(); 
        for (i, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(Expr::Function(f)) | SelectItem::ExprWithAlias { expr: Expr::Function(f), .. } => {
                    if f.over.is_none() {
                        agg_funcs.push((i, f.clone()));
                    }
                }
                _ => {}
            }
        }
        
        if let Some(having_expr) = &select.having {
            let extra_start = select.projection.len();
            collect_having_agg_funcs(having_expr, &mut agg_funcs, extra_start);
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
                
                for (i, item) in resolved_projection.iter().enumerate() {
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

        let window_results = if !window_funcs.is_empty() {
            Some(compute_window_functions(&filtered_rows, &schema, &window_funcs)?)
        } else {
            None
        };

        let (filtered_rows, window_results) = if !query.order_by.is_empty() {
            let mut indexed: Vec<(usize, Row)> = filtered_rows.into_iter().enumerate().collect();
            indexed.sort_by(|(_, a), (_, b)| {
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
            let reordered_wr = window_results.map(|wr| {
                indexed.iter().map(|(orig_idx, _)| wr[*orig_idx].clone()).collect()
            });
            let reordered_rows: Vec<Row> = indexed.into_iter().map(|(_, r)| r).collect();
            (reordered_rows, reordered_wr)
        } else {
            (filtered_rows, window_results)
        };

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
        
        // FETCH FIRST N ROWS ONLY (SQL standard, equivalent to LIMIT)
        if let Some(fetch) = &query.fetch {
            if let Some(quantity) = &fetch.quantity {
                if let Ok(v) = eval_expr(quantity, None, None) {
                    let n = match v { Value::Int64(n) => n as usize, Value::Int32(n) => n as usize, _ => 1 };
                    final_rows = final_rows.into_iter().take(n).collect();
                }
            } else {
                // FETCH FIRST ROW ONLY (no quantity means 1 row)
                final_rows = final_rows.into_iter().take(1).collect();
            }
        }

        let has_window_funcs = !window_funcs.is_empty();
        let wildcard = select.projection.iter().any(|p| matches!(p, SelectItem::Wildcard(_)));
        
        let mut cols = Vec::new();
        let mut result_rows = Vec::new();
        
        if wildcard && !has_window_funcs {
            for c in &schema.columns { cols.push(c.name.clone()); }
            result_rows = final_rows;
        } else {
            for item in &select.projection {
                cols.push(get_select_item_name(item));
            }
            
            for (row_idx, row) in final_rows.iter().enumerate() {
                let mut row_values = Vec::new();
                for (proj_idx, item) in resolved_projection.iter().enumerate() {
                    if let Some(wf_pos) = window_funcs.iter().position(|wf| wf.proj_idx == proj_idx) {
                        if let Some(ref wr) = window_results {
                            row_values.push(wr[row_idx][wf_pos].clone());
                        } else {
                            row_values.push(Value::Null);
                        }
                    } else {
                        let expr = match item {
                            SelectItem::UnnamedExpr(e) => e,
                            SelectItem::ExprWithAlias { expr: e, .. } => e,
                            SelectItem::Wildcard(_) => {
                                row_values.extend(row.values.clone());
                                continue;
                            }
                            _ => return Err(anyhow!("Unsupported select item")),
                        };
                        row_values.push(eval_expr(expr, Some(row), Some(&schema))?);
                    }
                }
                result_rows.push(Row::new(row_values));
            }
        }

        if select.distinct.is_some() {
            result_rows = dedup_rows(result_rows);
        }

        let result = ExecuteResult::Select { columns: cols, rows: result_rows };
        if let Some((target_name, _temp)) = select_into_target {
            return self.create_table_from_result(txn, &target_name, result).await;
        }
        Ok(result)
    }

    async fn execute_tableless_query(&self, txn: &mut Transaction, select: &sqlparser::ast::Select) -> Result<ExecuteResult> {
        let resolved_projection = self.resolve_projection_subqueries(txn, &select.projection).await?;
        
        let mut cols = Vec::new();
        let mut values = Vec::new();
        
        for item in &resolved_projection {
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

    fn execute_set_operation<'a>(
        &'a self,
        txn: &'a mut Transaction,
        op: &'a SetOperator,
        quantifier: &'a SetQuantifier,
        left: &'a SetExpr,
        right: &'a SetExpr,
        ctes: &'a HashMap<String, (TableSchema, Vec<Row>)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecuteResult>> + Send + 'a>> {
        Box::pin(async move {
            let left_result = self.execute_set_expr(txn, left, ctes).await?;
            let right_result = self.execute_set_expr(txn, right, ctes).await?;

        let (left_cols, left_rows) = match left_result {
            ExecuteResult::Select { columns, rows } => (columns, rows),
            _ => return Err(anyhow!("Left side of set operation must be SELECT")),
        };
        let (right_cols, right_rows) = match right_result {
            ExecuteResult::Select { columns, rows } => (columns, rows),
            _ => return Err(anyhow!("Right side of set operation must be SELECT")),
        };

        if left_cols.len() != right_cols.len() {
            return Err(anyhow!("Column count mismatch in set operation"));
        }

        let columns = left_cols;
        let is_all = matches!(quantifier, SetQuantifier::All);

        let rows = match op {
            SetOperator::Union => {
                let mut result = left_rows;
                if is_all {
                    result.extend(right_rows);
                } else {
                    let existing: std::collections::HashSet<Vec<u8>> = result
                        .iter()
                        .map(|r| bincode::serialize(&r.values).unwrap_or_default())
                        .collect();
                    for row in right_rows {
                        let key = bincode::serialize(&row.values).unwrap_or_default();
                        if !existing.contains(&key) {
                            result.push(row);
                        }
                    }
                    let mut seen = std::collections::HashSet::new();
                    result.retain(|r| {
                        let key = bincode::serialize(&r.values).unwrap_or_default();
                        seen.insert(key)
                    });
                }
                result
            }
            SetOperator::Intersect => {
                let right_set: std::collections::HashSet<Vec<u8>> = right_rows
                    .iter()
                    .map(|r| bincode::serialize(&r.values).unwrap_or_default())
                    .collect();
                let mut result: Vec<Row> = left_rows
                    .into_iter()
                    .filter(|r| {
                        let key = bincode::serialize(&r.values).unwrap_or_default();
                        right_set.contains(&key)
                    })
                    .collect();
                if !is_all {
                    let mut seen = std::collections::HashSet::new();
                    result.retain(|r| {
                        let key = bincode::serialize(&r.values).unwrap_or_default();
                        seen.insert(key)
                    });
                }
                result
            }
            SetOperator::Except => {
                let right_set: std::collections::HashSet<Vec<u8>> = right_rows
                    .iter()
                    .map(|r| bincode::serialize(&r.values).unwrap_or_default())
                    .collect();
                let mut result: Vec<Row> = left_rows
                    .into_iter()
                    .filter(|r| {
                        let key = bincode::serialize(&r.values).unwrap_or_default();
                        !right_set.contains(&key)
                    })
                    .collect();
                if !is_all {
                    let mut seen = std::collections::HashSet::new();
                    result.retain(|r| {
                        let key = bincode::serialize(&r.values).unwrap_or_default();
                        seen.insert(key)
                    });
                }
                result
            }
        };

            Ok(ExecuteResult::Select { columns, rows })
        })
    }

    fn execute_set_expr<'a>(
        &'a self,
        txn: &'a mut Transaction,
        expr: &'a SetExpr,
        ctes: &'a HashMap<String, (TableSchema, Vec<Row>)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecuteResult>> + Send + 'a>> {
        Box::pin(async move {
            match expr {
                SetExpr::Select(s) => {
                    let query = Query {
                        with: None,
                        body: Box::new(SetExpr::Select(s.clone())),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        limit_by: vec![],
                        for_clause: None,
                    };
                    self.execute_query_with_ctes(txn, &query, ctes).await
                }
                SetExpr::SetOperation { op, set_quantifier, left, right } => {
                    self.execute_set_operation(txn, op, set_quantifier, left, right, ctes).await
                }
                _ => Err(anyhow!("Unsupported set expression")),
            }
        })
    }

    async fn execute_join_query(&self, txn: &mut Transaction, query: &Query, select: &sqlparser::ast::Select) -> Result<ExecuteResult> {
        self.execute_join_query_with_ctes(txn, query, select, &HashMap::new()).await
    }

    async fn get_table_data(&self, txn: &mut Transaction, table_name: &str, ctes: &HashMap<String, (TableSchema, Vec<Row>)>) -> Result<(TableSchema, Vec<Row>)> {
        let t_lower = table_name.to_lowercase();
        if let Some((schema, rows)) = ctes.get(&t_lower) {
            return Ok((schema.clone(), rows.clone()));
        }
        
        if let Some(view_query) = self.store.get_view(txn, &t_lower).await? {
            let result = self.execute_view_query(txn, &view_query, ctes).await?;
            return match result {
                ExecuteResult::Select { columns, rows } => {
                    let schema = TableSchema {
                        table_id: 0,
                        name: t_lower,
                        columns: columns.iter().map(|n| ColumnDef { 
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
                    Ok((schema, rows))
                }
                _ => Err(anyhow!("View must return SELECT result")),
            };
        }
        
        let schema = self.store.get_schema(txn, table_name).await?
            .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;
        let rows = self.scan_and_fill(txn, table_name, &schema).await?;
        Ok((schema, rows))
    }

    fn execute_view_query<'a>(&'a self, txn: &'a mut Transaction, view_query: &'a str, ctes: &'a HashMap<String, (TableSchema, Vec<Row>)>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecuteResult>> + Send + 'a>> {
        Box::pin(async move {
            let ast = parse_sql(view_query)?;
            if let Some(Statement::Query(q)) = ast.into_iter().next() {
                self.execute_query_with_ctes(txn, &q, ctes).await
            } else {
                Err(anyhow!("Invalid view query"))
            }
        })
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
        let mut has_natural_join = false;
        let mut natural_join_common_cols: Vec<String> = Vec::new();

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

            let left_columns: Vec<String> = combined_schemas.iter()
                .flat_map(|(_, s)| s.columns.iter().map(|c| c.name.clone()))
                .collect();
            let right_columns: Vec<String> = join_schema.columns.iter().map(|c| c.name.clone()).collect();
            
            let (join_condition, is_natural) = match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr)) => (Some(expr.clone()), false),
                JoinOperator::LeftOuter(JoinConstraint::On(expr)) => (Some(expr.clone()), false),
                JoinOperator::RightOuter(JoinConstraint::On(expr)) => (Some(expr.clone()), false),
                JoinOperator::FullOuter(JoinConstraint::On(expr)) => (Some(expr.clone()), false),
                JoinOperator::Inner(JoinConstraint::Natural) |
                JoinOperator::LeftOuter(JoinConstraint::Natural) |
                JoinOperator::RightOuter(JoinConstraint::Natural) |
                JoinOperator::FullOuter(JoinConstraint::Natural) => {
                    let common_cols: Vec<String> = left_columns.iter()
                        .filter(|c| right_columns.contains(c))
                        .cloned()
                        .collect();
                    has_natural_join = true;
                    natural_join_common_cols = common_cols.clone();
                    if common_cols.is_empty() {
                        (None, true)
                    } else {
                        let cond = common_cols.iter()
                            .map(|col| Expr::BinaryOp {
                                left: Box::new(Expr::CompoundIdentifier(vec![
                                    Ident::new(base_alias.clone()),
                                    Ident::new(col.clone()),
                                ])),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::CompoundIdentifier(vec![
                                    Ident::new(join_alias.clone()),
                                    Ident::new(col.clone()),
                                ])),
                            })
                            .reduce(|a, b| Expr::BinaryOp {
                                left: Box::new(a),
                                op: BinaryOperator::And,
                                right: Box::new(b),
                            })
                            .unwrap();
                        (Some(cond), true)
                    }
                },
                JoinOperator::CrossJoin => (None, false),
                _ => return Err(anyhow!("Unsupported JOIN type")),
            };
            let _ = is_natural;

            let is_left_join = matches!(&join.join_operator, JoinOperator::LeftOuter(_));
            let is_right_join = matches!(&join.join_operator, JoinOperator::RightOuter(_));
            let is_full_join = matches!(&join.join_operator, JoinOperator::FullOuter(_));
            
            let left_col_count: usize = combined_schemas.iter().map(|(_, s)| s.columns.len()).sum();
            
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
            let _join_start_offset = offset;
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
            let mut right_matched: Vec<bool> = vec![false; join_rows.len()];
            
            for left_row in &combined_rows {
                let mut matched = false;
                for (right_idx, right_row) in join_rows.iter().enumerate() {
                    let mut combined_values = left_row.values.clone();
                    combined_values.extend(right_row.values.clone());
                    let combined_row = Row::new(combined_values);

                    let matches = if let Some(ref cond) = join_condition {
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
                        right_matched[right_idx] = true;
                    }
                }
                if (is_left_join || is_full_join) && !matched {
                    let mut combined_values = left_row.values.clone();
                    for _ in 0..join_schema.columns.len() {
                        combined_values.push(Value::Null);
                    }
                    new_combined_rows.push(Row::new(combined_values));
                }
            }
            
            if is_right_join || is_full_join {
                for (right_idx, right_row) in join_rows.iter().enumerate() {
                    if !right_matched[right_idx] {
                        let mut combined_values: Vec<Value> = Vec::new();
                        for _ in 0..left_col_count {
                            combined_values.push(Value::Null);
                        }
                        combined_values.extend(right_row.values.clone());
                        new_combined_rows.push(Row::new(combined_values));
                    }
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

        let resolved_projection = self.resolve_projection_subqueries(txn, &select.projection).await?;

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
        
        if let Some(having_expr) = &select.having {
            let extra_start = select.projection.len();
            collect_having_agg_funcs(having_expr, &mut agg_funcs, extra_start);
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
                for (i, item) in resolved_projection.iter().enumerate() {
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
            if has_natural_join && !natural_join_common_cols.is_empty() {
                let mut col_indices_to_keep: Vec<usize> = Vec::new();
                let mut seen_common_cols: std::collections::HashSet<String> = std::collections::HashSet::new();
                
                for common_col in &natural_join_common_cols {
                    cols.push(common_col.clone());
                }
                
                let mut offset = 0;
                for (_, schema) in &combined_schemas {
                    for col in &schema.columns {
                        if natural_join_common_cols.contains(&col.name) {
                            if !seen_common_cols.contains(&col.name) {
                                col_indices_to_keep.push(offset);
                                seen_common_cols.insert(col.name.clone());
                            }
                        } else {
                            cols.push(col.name.clone());
                            col_indices_to_keep.push(offset);
                        }
                        offset += 1;
                    }
                }
                
                result_rows = final_rows.into_iter().map(|row| {
                    let vals: Vec<Value> = col_indices_to_keep.iter()
                        .map(|&idx| row.values.get(idx).cloned().unwrap_or(Value::Null))
                        .collect();
                    Row::new(vals)
                }).collect();
            } else {
                for (alias, schema) in &combined_schemas {
                    for col in &schema.columns {
                        cols.push(format!("{}.{}", alias, col.name));
                    }
                }
                result_rows = final_rows;
            }
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
                for item in &resolved_projection {
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
                Expr::Subquery(subquery) => {
                    let result = self.execute_query(txn, subquery).await?;
                    match result {
                        ExecuteResult::Select { rows, .. } => {
                            if rows.is_empty() {
                                Ok(Expr::Value(SqlValue::Null))
                            } else if rows.len() == 1 {
                                let value = rows[0].values.first().cloned().unwrap_or(Value::Null);
                                Ok(value_to_sql_expr(&value))
                            } else {
                                Err(anyhow!("Scalar subquery returned more than one row"))
                            }
                        }
                        _ => Err(anyhow!("Subquery must return a SELECT result")),
                    }
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

    async fn resolve_projection_subqueries(&self, txn: &mut Transaction, projection: &[SelectItem]) -> Result<Vec<SelectItem>> {
        let mut resolved = Vec::with_capacity(projection.len());
        for item in projection {
            let resolved_item = match item {
                SelectItem::UnnamedExpr(e) => {
                    SelectItem::UnnamedExpr(self.resolve_subqueries(txn, e).await?)
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    SelectItem::ExprWithAlias {
                        expr: self.resolve_subqueries(txn, expr).await?,
                        alias: alias.clone(),
                    }
                }
                other => other.clone(),
            };
            resolved.push(resolved_item);
        }
        Ok(resolved)
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
            DataType::Array(_) => {
                if let Ok(arr) = parse_pg_array(&unescaped) {
                    Value::Array(arr)
                } else {
                    Value::Text(unescaped)
                }
            }
            DataType::Json => {
                if serde_json::from_str::<serde_json::Value>(&unescaped).is_ok() {
                    Value::Json(unescaped)
                } else {
                    Value::Text(unescaped)
                }
            }
            DataType::Jsonb => {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&unescaped) {
                    Value::Jsonb(parsed.to_string())
                } else {
                    Value::Text(unescaped)
                }
            }
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

    async fn execute_create_role(
        &self,
        txn: &mut Transaction,
        names: &[ObjectName],
        if_not_exists: bool,
        login: &Option<bool>,
        _inherit: &Option<bool>,
        password: &Option<SqlPassword>,
        superuser: &Option<bool>,
        create_db: &Option<bool>,
        create_role: &Option<bool>,
        _connection_limit: &Option<Expr>,
        _valid_until: &Option<Expr>,
    ) -> Result<ExecuteResult> {
        for name in names {
            let role_name = name.0.last().ok_or_else(|| anyhow!("Invalid role name"))?.value.clone();
            
            if if_not_exists {
                if self.auth_manager.get_user(txn, &role_name).await?.is_some() {
                    continue;
                }
            }

            let pwd = match password {
                Some(SqlPassword::Password(expr)) => {
                    if let Expr::Value(SqlValue::SingleQuotedString(s)) = expr {
                        s.clone()
                    } else {
                        "".to_string()
                    }
                }
                Some(SqlPassword::NullPassword) => "".to_string(),
                None => "".to_string(),
            };

            let is_superuser = superuser.unwrap_or(false);
            let can_login = login.unwrap_or(false);

            let mut user = if is_superuser {
                User::new_superuser(&role_name, &pwd)
            } else {
                User::new(&role_name, &pwd)
            };

            user.can_login = can_login;
            user.can_create_db = create_db.unwrap_or(false);
            user.can_create_role = create_role.unwrap_or(false);

            self.auth_manager.create_user(txn, user).await?;
        }

        Ok(ExecuteResult::CreateRole)
    }

    async fn execute_alter_role(
        &self,
        txn: &mut Transaction,
        name: &Ident,
        operation: &AlterRoleOperation,
    ) -> Result<ExecuteResult> {
        let role_name = name.value.clone();
        let mut user = self.auth_manager.get_user(txn, &role_name).await?
            .ok_or_else(|| anyhow!("Role '{}' does not exist", role_name))?;

        match operation {
            AlterRoleOperation::RenameRole { role_name: new_name } => {
                self.auth_manager.drop_user(txn, &role_name).await?;
                user.name = new_name.value.clone();
                self.auth_manager.create_user(txn, user).await?;
            }
            AlterRoleOperation::WithOptions { options } => {
                for opt in options {
                    match opt {
                        sqlparser::ast::RoleOption::SuperUser(v) => user.is_superuser = *v,
                        sqlparser::ast::RoleOption::CreateDB(v) => user.can_create_db = *v,
                        sqlparser::ast::RoleOption::CreateRole(v) => user.can_create_role = *v,
                        sqlparser::ast::RoleOption::Login(v) => user.can_login = *v,
                        sqlparser::ast::RoleOption::Password(p) => {
                            if let SqlPassword::Password(expr) = p {
                                if let Expr::Value(SqlValue::SingleQuotedString(s)) = expr {
                                    user.set_password(s);
                                }
                            }
                        }
                        sqlparser::ast::RoleOption::ConnectionLimit(expr) => {
                            if let Expr::Value(SqlValue::Number(n, _)) = expr {
                                user.connection_limit = n.parse().unwrap_or(-1);
                            }
                        }
                        _ => {}
                    }
                }
                self.auth_manager.update_user(txn, user).await?;
            }
            AlterRoleOperation::AddMember { member_name } => {
                self.auth_manager.grant_role_to_user(txn, &member_name.value, &role_name).await?;
            }
            AlterRoleOperation::DropMember { member_name } => {
                self.auth_manager.revoke_role_from_user(txn, &member_name.value, &role_name).await?;
            }
            _ => {}
        }

        Ok(ExecuteResult::AlterRole)
    }

    async fn execute_drop_role(
        &self,
        txn: &mut Transaction,
        names: &[ObjectName],
        if_exists: bool,
    ) -> Result<ExecuteResult> {
        for name in names {
            let role_name = name.0.last().ok_or_else(|| anyhow!("Invalid role name"))?.value.clone();
            
            let dropped = self.auth_manager.drop_user(txn, &role_name).await?;
            if !dropped && !if_exists {
                return Err(anyhow!("Role '{}' does not exist", role_name));
            }
            
            if !dropped {
                let role_dropped = self.auth_manager.drop_role(txn, &role_name).await?;
                if !role_dropped && !if_exists {
                    return Err(anyhow!("Role '{}' does not exist", role_name));
                }
            }
        }
        
        Ok(ExecuteResult::DropRole)
    }

    async fn execute_grant(
        &self,
        txn: &mut Transaction,
        privileges: &Privileges,
        objects: &Option<GrantObjects>,
        grantees: &[Ident],
        with_grant_option: bool,
    ) -> Result<ExecuteResult> {
        let privs = match privileges {
            Privileges::All { .. } => vec![Privilege::All],
            Privileges::Actions(actions) => {
                actions.iter().filter_map(|a| {
                    match a {
                        sqlparser::ast::Action::Select { .. } => Some(Privilege::Select),
                        sqlparser::ast::Action::Insert { .. } => Some(Privilege::Insert),
                        sqlparser::ast::Action::Update { .. } => Some(Privilege::Update),
                        sqlparser::ast::Action::Delete { .. } => Some(Privilege::Delete),
                        sqlparser::ast::Action::Truncate => Some(Privilege::Truncate),
                        sqlparser::ast::Action::References { .. } => Some(Privilege::References),
                        sqlparser::ast::Action::Trigger => Some(Privilege::Trigger),
                        sqlparser::ast::Action::Connect => Some(Privilege::Connect),
                        sqlparser::ast::Action::Create => Some(Privilege::CreateTable),
                        sqlparser::ast::Action::Execute => Some(Privilege::Execute),
                        sqlparser::ast::Action::Usage => Some(Privilege::Usage),
                        _ => None,
                    }
                }).collect()
            }
        };

        let obj = match objects {
            Some(GrantObjects::Tables(tables)) => {
                if tables.is_empty() {
                    PrivilegeObject::Global
                } else {
                    let table_name = tables[0].0.last()
                        .map(|i| i.value.clone())
                        .unwrap_or_default();
                    PrivilegeObject::table(&table_name)
                }
            }
            Some(GrantObjects::AllTablesInSchema { schemas }) => {
                let schema = schemas.first()
                    .map(|s| s.0.last().map(|i| i.value.clone()).unwrap_or("public".to_string()))
                    .unwrap_or("public".to_string());
                PrivilegeObject::AllTablesInSchema(schema)
            }
            Some(GrantObjects::Schemas(schemas)) => {
                let schema = schemas.first()
                    .map(|s| s.0.last().map(|i| i.value.clone()).unwrap_or("public".to_string()))
                    .unwrap_or("public".to_string());
                PrivilegeObject::Schema(schema)
            }
            _ => PrivilegeObject::Global,
        };

        for grantee in grantees {
            let username = grantee.value.clone();
            if let Some(mut user) = self.auth_manager.get_user(txn, &username).await? {
                for priv_type in &privs {
                    user.grant_privilege(priv_type.clone(), obj.clone(), with_grant_option);
                }
                self.auth_manager.update_user(txn, user).await?;
            } else if let Some(mut role) = self.auth_manager.get_role(txn, &username).await? {
                for priv_type in &privs {
                    role.privileges.push(crate::auth::GrantedPrivilege {
                        privilege: priv_type.clone(),
                        object: obj.clone(),
                        with_grant_option,
                    });
                }
                self.auth_manager.update_role(txn, role).await?;
            } else {
                return Err(anyhow!("Role or user '{}' does not exist", username));
            }
        }

        Ok(ExecuteResult::Grant)
    }

    async fn execute_revoke(
        &self,
        txn: &mut Transaction,
        privileges: &Privileges,
        objects: &Option<GrantObjects>,
        grantees: &[Ident],
    ) -> Result<ExecuteResult> {
        let privs: Vec<Privilege> = match privileges {
            Privileges::All { .. } => vec![Privilege::All],
            Privileges::Actions(actions) => {
                actions.iter().filter_map(|a| {
                    match a {
                        sqlparser::ast::Action::Select { .. } => Some(Privilege::Select),
                        sqlparser::ast::Action::Insert { .. } => Some(Privilege::Insert),
                        sqlparser::ast::Action::Update { .. } => Some(Privilege::Update),
                        sqlparser::ast::Action::Delete { .. } => Some(Privilege::Delete),
                        sqlparser::ast::Action::Truncate => Some(Privilege::Truncate),
                        sqlparser::ast::Action::References { .. } => Some(Privilege::References),
                        sqlparser::ast::Action::Trigger => Some(Privilege::Trigger),
                        sqlparser::ast::Action::Connect => Some(Privilege::Connect),
                        sqlparser::ast::Action::Create => Some(Privilege::CreateTable),
                        sqlparser::ast::Action::Execute => Some(Privilege::Execute),
                        sqlparser::ast::Action::Usage => Some(Privilege::Usage),
                        _ => None,
                    }
                }).collect()
            }
        };

        let obj = match objects {
            Some(GrantObjects::Tables(tables)) => {
                if tables.is_empty() {
                    PrivilegeObject::Global
                } else {
                    let table_name = tables[0].0.last()
                        .map(|i| i.value.clone())
                        .unwrap_or_default();
                    PrivilegeObject::table(&table_name)
                }
            }
            Some(GrantObjects::AllTablesInSchema { schemas }) => {
                let schema = schemas.first()
                    .map(|s| s.0.last().map(|i| i.value.clone()).unwrap_or("public".to_string()))
                    .unwrap_or("public".to_string());
                PrivilegeObject::AllTablesInSchema(schema)
            }
            Some(GrantObjects::Schemas(schemas)) => {
                let schema = schemas.first()
                    .map(|s| s.0.last().map(|i| i.value.clone()).unwrap_or("public".to_string()))
                    .unwrap_or("public".to_string());
                PrivilegeObject::Schema(schema)
            }
            _ => PrivilegeObject::Global,
        };

        for grantee in grantees {
            let username = grantee.value.clone();
            if let Some(mut user) = self.auth_manager.get_user(txn, &username).await? {
                for priv_type in &privs {
                    user.revoke_privilege(priv_type, &obj);
                }
                self.auth_manager.update_user(txn, user).await?;
            } else if let Some(mut role) = self.auth_manager.get_role(txn, &username).await? {
                role.privileges.retain(|p| !privs.contains(&p.privilege) || p.object != obj);
                self.auth_manager.update_role(txn, role).await?;
            } else {
                return Err(anyhow!("Role or user '{}' does not exist", username));
            }
        }

        Ok(ExecuteResult::Revoke)
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

fn coerce_value_for_column(val: Value, col: &crate::types::ColumnDef) -> Result<Value> {
    match (&val, &col.data_type) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Text(s), DataType::Json) => {
            serde_json::from_str::<serde_json::Value>(s)
                .map_err(|e| anyhow!("invalid input syntax for type json: {}", e))?;
            Ok(Value::Json(s.clone()))
        }
        (Value::Text(s), DataType::Jsonb) => {
            let parsed: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| anyhow!("invalid input syntax for type jsonb: {}", e))?;
            Ok(Value::Jsonb(parsed.to_string()))
        }
        (Value::Json(s), DataType::Json) => Ok(Value::Json(s.clone())),
        (Value::Json(s), DataType::Jsonb) => {
            let parsed: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| anyhow!("invalid input syntax for type jsonb: {}", e))?;
            Ok(Value::Jsonb(parsed.to_string()))
        }
        (Value::Jsonb(s), DataType::Json) => Ok(Value::Json(s.clone())),
        (Value::Jsonb(s), DataType::Jsonb) => Ok(Value::Jsonb(s.clone())),
        _ => Ok(val),
    }
}

struct WindowFuncInfo {
    proj_idx: usize,
    func_name: String,
    arg_expr: Option<Expr>,
    partition_by: Vec<Expr>,
    order_by: Vec<OrderByExpr>,
    offset_expr: Option<Expr>,
    default_value_expr: Option<Expr>,
}

fn extract_window_functions(projection: &[SelectItem]) -> Vec<WindowFuncInfo> {
    let mut result = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        let func = match item {
            SelectItem::UnnamedExpr(Expr::Function(f)) => Some(f),
            SelectItem::ExprWithAlias { expr: Expr::Function(f), .. } => Some(f),
            _ => None,
        };
        if let Some(f) = func {
            if let Some(WindowType::WindowSpec(spec)) = &f.over {
                let func_name = f.name.0.last().map(|i| i.value.to_lowercase()).unwrap_or_default();
                let extract_arg = |index: usize| -> Option<Expr> {
                    f.args.get(index).and_then(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
                        _ => None,
                    })
                };
                let arg_expr = extract_arg(0);
                let offset_expr = extract_arg(1);
                let default_value_expr = extract_arg(2);
                result.push(WindowFuncInfo {
                    proj_idx: idx,
                    func_name,
                    arg_expr,
                    partition_by: spec.partition_by.clone(),
                    order_by: spec.order_by.clone(),
                    offset_expr,
                    default_value_expr,
                });
            }
        }
    }
    result
}

fn compute_window_functions(
    rows: &[Row],
    schema: &TableSchema,
    window_funcs: &[WindowFuncInfo],
) -> Result<Vec<Vec<Value>>> {
    let mut results: Vec<Vec<Value>> = vec![vec![Value::Null; window_funcs.len()]; rows.len()];
    
    for (wf_idx, wf) in window_funcs.iter().enumerate() {
        let mut partitions: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for (row_idx, row) in rows.iter().enumerate() {
            let mut key = Vec::new();
            for expr in &wf.partition_by {
                key.push(eval_expr(expr, Some(row), Some(schema))?);
            }
            let key_bytes = bincode::serialize(&key).unwrap_or_default();
            partitions.entry(key_bytes).or_default().push(row_idx);
        }
        
        for (_partition_key, mut row_indices) in partitions {
            if !wf.order_by.is_empty() {
                row_indices.sort_by(|&a, &b| {
                    for order_expr in &wf.order_by {
                        let val_a = eval_expr(&order_expr.expr, Some(&rows[a]), Some(schema)).unwrap_or(Value::Null);
                        let val_b = eval_expr(&order_expr.expr, Some(&rows[b]), Some(schema)).unwrap_or(Value::Null);
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
            
            match wf.func_name.as_str() {
                "row_number" => {
                    for (pos, &row_idx) in row_indices.iter().enumerate() {
                        results[row_idx][wf_idx] = Value::Int64((pos + 1) as i64);
                    }
                }
                "rank" => {
                    let mut current_rank = 1i64;
                    let mut prev_values: Option<Vec<Value>> = None;
                    for (pos, &row_idx) in row_indices.iter().enumerate() {
                        let current_values: Vec<Value> = wf.order_by.iter()
                            .map(|o| eval_expr(&o.expr, Some(&rows[row_idx]), Some(schema)).unwrap_or(Value::Null))
                            .collect();
                        if let Some(prev) = &prev_values {
                            if prev != &current_values {
                                current_rank = (pos + 1) as i64;
                            }
                        }
                        results[row_idx][wf_idx] = Value::Int64(current_rank);
                        prev_values = Some(current_values);
                    }
                }
                "dense_rank" => {
                    let mut current_rank = 1i64;
                    let mut prev_values: Option<Vec<Value>> = None;
                    for &row_idx in &row_indices {
                        let current_values: Vec<Value> = wf.order_by.iter()
                            .map(|o| eval_expr(&o.expr, Some(&rows[row_idx]), Some(schema)).unwrap_or(Value::Null))
                            .collect();
                        if let Some(prev) = &prev_values {
                            if prev != &current_values {
                                current_rank += 1;
                            }
                        }
                        results[row_idx][wf_idx] = Value::Int64(current_rank);
                        prev_values = Some(current_values);
                    }
                }
                "sum" => {
                    if wf.order_by.is_empty() {
                        let mut total = 0.0f64;
                        for &row_idx in &row_indices {
                            if let Some(ref arg) = wf.arg_expr {
                                let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                                match val {
                                    Value::Int32(n) => total += n as f64,
                                    Value::Int64(n) => total += n as f64,
                                    Value::Float64(n) => total += n,
                                    _ => {}
                                }
                            }
                        }
                        for &row_idx in &row_indices {
                            results[row_idx][wf_idx] = Value::Float64(total);
                        }
                    } else {
                        let mut running_sum = 0.0f64;
                        for &row_idx in &row_indices {
                            if let Some(ref arg) = wf.arg_expr {
                                let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                                match val {
                                    Value::Int32(n) => running_sum += n as f64,
                                    Value::Int64(n) => running_sum += n as f64,
                                    Value::Float64(n) => running_sum += n,
                                    _ => {}
                                }
                            }
                            results[row_idx][wf_idx] = Value::Float64(running_sum);
                        }
                    }
                }
                "count" => {
                    if wf.order_by.is_empty() {
                        let total = row_indices.len() as i64;
                        for &row_idx in &row_indices {
                            results[row_idx][wf_idx] = Value::Int64(total);
                        }
                    } else {
                        let mut running_count = 0i64;
                        for &row_idx in &row_indices {
                            running_count += 1;
                            results[row_idx][wf_idx] = Value::Int64(running_count);
                        }
                    }
                }
                "avg" => {
                    if wf.order_by.is_empty() {
                        let mut total_sum = 0.0f64;
                        let mut total_count = 0i64;
                        for &row_idx in &row_indices {
                            if let Some(ref arg) = wf.arg_expr {
                                let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                                match val {
                                    Value::Int32(n) => { total_sum += n as f64; total_count += 1; }
                                    Value::Int64(n) => { total_sum += n as f64; total_count += 1; }
                                    Value::Float64(n) => { total_sum += n; total_count += 1; }
                                    Value::Null => {}
                                    _ => { total_count += 1; }
                                }
                            }
                        }
                        let avg_val = if total_count > 0 { Value::Float64(total_sum / total_count as f64) } else { Value::Null };
                        for &row_idx in &row_indices {
                            results[row_idx][wf_idx] = avg_val.clone();
                        }
                    } else {
                        let mut running_sum = 0.0f64;
                        let mut running_count = 0i64;
                        for &row_idx in &row_indices {
                            if let Some(ref arg) = wf.arg_expr {
                                let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                                match val {
                                    Value::Int32(n) => { running_sum += n as f64; running_count += 1; }
                                    Value::Int64(n) => { running_sum += n as f64; running_count += 1; }
                                    Value::Float64(n) => { running_sum += n; running_count += 1; }
                                    Value::Null => {}
                                    _ => { running_count += 1; }
                                }
                            }
                            results[row_idx][wf_idx] = if running_count > 0 {
                                Value::Float64(running_sum / running_count as f64)
                            } else {
                                Value::Null
                            };
                        }
                    }
                }
                "min" => {
                    let mut min_val: Option<Value> = None;
                    for &row_idx in &row_indices {
                        if let Some(ref arg) = wf.arg_expr {
                            let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                            if !matches!(val, Value::Null) {
                                min_val = Some(match &min_val {
                                    None => val.clone(),
                                    Some(m) => if super::expr::compare_values(&val, m).unwrap_or(0) < 0 { val.clone() } else { m.clone() },
                                });
                            }
                        }
                        if !wf.order_by.is_empty() {
                            results[row_idx][wf_idx] = min_val.clone().unwrap_or(Value::Null);
                        }
                    }
                    if wf.order_by.is_empty() {
                        let final_min = min_val.unwrap_or(Value::Null);
                        for &row_idx in &row_indices {
                            results[row_idx][wf_idx] = final_min.clone();
                        }
                    }
                }
                "max" => {
                    let mut max_val: Option<Value> = None;
                    for &row_idx in &row_indices {
                        if let Some(ref arg) = wf.arg_expr {
                            let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                            if !matches!(val, Value::Null) {
                                max_val = Some(match &max_val {
                                    None => val.clone(),
                                    Some(m) => if super::expr::compare_values(&val, m).unwrap_or(0) > 0 { val.clone() } else { m.clone() },
                                });
                            }
                        }
                        if !wf.order_by.is_empty() {
                            results[row_idx][wf_idx] = max_val.clone().unwrap_or(Value::Null);
                        }
                    }
                    if wf.order_by.is_empty() {
                        let final_max = max_val.unwrap_or(Value::Null);
                        for &row_idx in &row_indices {
                            results[row_idx][wf_idx] = final_max.clone();
                        }
                    }
                }
                "lag" => {
                    let offset = wf.offset_expr.as_ref()
                        .and_then(|e| match e {
                            Expr::Value(SqlValue::Number(n, _)) => n.parse::<usize>().ok(),
                            _ => None,
                        })
                        .unwrap_or(1);
                    let default_val = wf.default_value_expr.as_ref()
                        .map(|e| eval_expr(e, None, None).unwrap_or(Value::Null))
                        .unwrap_or(Value::Null);
                    
                    for (pos, &row_idx) in row_indices.iter().enumerate() {
                        let val = if pos >= offset {
                            let lag_row_idx = row_indices[pos - offset];
                            if let Some(ref arg) = wf.arg_expr {
                                eval_expr(arg, Some(&rows[lag_row_idx]), Some(schema))?
                            } else {
                                Value::Null
                            }
                        } else {
                            default_val.clone()
                        };
                        results[row_idx][wf_idx] = val;
                    }
                }
                "lead" => {
                    let offset = wf.offset_expr.as_ref()
                        .and_then(|e| match e {
                            Expr::Value(SqlValue::Number(n, _)) => n.parse::<usize>().ok(),
                            _ => None,
                        })
                        .unwrap_or(1);
                    let default_val = wf.default_value_expr.as_ref()
                        .map(|e| eval_expr(e, None, None).unwrap_or(Value::Null))
                        .unwrap_or(Value::Null);
                    
                    for (pos, &row_idx) in row_indices.iter().enumerate() {
                        let val = if pos + offset < row_indices.len() {
                            let lead_row_idx = row_indices[pos + offset];
                            if let Some(ref arg) = wf.arg_expr {
                                eval_expr(arg, Some(&rows[lead_row_idx]), Some(schema))?
                            } else {
                                Value::Null
                            }
                        } else {
                            default_val.clone()
                        };
                        results[row_idx][wf_idx] = val;
                    }
                }
                _ => {
                    return Err(anyhow!("Unsupported window function: {}", wf.func_name));
                }
            }
        }
    }
    
    Ok(results)
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
        Value::Array(elems) => {
            let elem_exprs: Vec<Expr> = elems.iter().map(value_to_sql_expr).collect();
            Expr::Array(sqlparser::ast::Array {
                elem: elem_exprs,
                named: true,
            })
        }
        Value::Json(s) => Expr::Value(SqlValue::SingleQuotedString(s.clone())),
        Value::Jsonb(s) => Expr::Value(SqlValue::SingleQuotedString(s.clone())),
    }
}

fn parse_pg_array(s: &str) -> Result<Vec<Value>> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return Err(anyhow!("Invalid array format"));
    }

    let inner = &s[1..s.len() - 1];
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;

    for c in inner.chars() {
        if escape_next {
            current.push(c);
            escape_next = false;
            continue;
        }

        match c {
            '\\' => escape_next = true,
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                let val = parse_array_element(&current);
                result.push(val);
                current.clear();
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() || inner.ends_with(',') {
        let val = parse_array_element(&current);
        result.push(val);
    }

    Ok(result)
}

fn parse_array_element(s: &str) -> Value {
    let s = s.trim();
    if s.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }

    if let Ok(i) = s.parse::<i32>() {
        return Value::Int32(i);
    }
    if let Ok(i) = s.parse::<i64>() {
        return Value::Int64(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float64(f);
    }
    if s.eq_ignore_ascii_case("true") {
        return Value::Boolean(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return Value::Boolean(false);
    }

    Value::Text(s.to_string())
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
                    "JSON" => Ok(DataType::Json),
                    "JSONB" => Ok(DataType::Jsonb),
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

fn collect_having_agg_funcs(expr: &Expr, agg_funcs: &mut Vec<(usize, sqlparser::ast::Function)>, extra_start: usize) {
    debug!("collect_having_agg_funcs called with expr: {:?}", expr);
    match expr {
        Expr::Function(f) if f.over.is_none() => {
            let func_name = f.name.0.last().map(|i| i.value.to_uppercase()).unwrap_or_default();
            debug!("Found function in HAVING: {}", func_name);
            if matches!(func_name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                let already_exists = agg_funcs.iter().any(|(_, existing)| {
                    let existing_name = existing.name.0.last().map(|n| n.value.to_uppercase()).unwrap_or_default();
                    existing_name == func_name && args_match(f, existing)
                });
                debug!("Already exists: {}, agg_funcs len: {}", already_exists, agg_funcs.len());
                if !already_exists {
                    let new_idx = extra_start + (agg_funcs.len() - agg_funcs.iter().filter(|(idx, _)| *idx < extra_start).count());
                    debug!("Adding new agg func at index {}", new_idx);
                    agg_funcs.push((new_idx, f.clone()));
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_having_agg_funcs(left, agg_funcs, extra_start);
            collect_having_agg_funcs(right, agg_funcs, extra_start);
        }
        Expr::Nested(e) => collect_having_agg_funcs(e, agg_funcs, extra_start),
        _ => {
            debug!("Other expr type in HAVING: {:?}", expr);
        }
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

fn infer_data_type(value: &Value) -> DataType {
    match value {
        Value::Int32(_) => DataType::Int32,
        Value::Int64(_) => DataType::Int64,
        Value::Float64(_) => DataType::Float64,
        Value::Boolean(_) => DataType::Boolean,
        Value::Text(_) => DataType::Text,
        Value::Bytes(_) => DataType::Bytes,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::Interval { .. } => DataType::Interval,
        Value::Uuid(_) => DataType::Uuid,
        Value::Json(_) => DataType::Json,
        Value::Jsonb(_) => DataType::Jsonb,
        Value::Array(_) => DataType::Text,
        Value::Null => DataType::Text,
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
