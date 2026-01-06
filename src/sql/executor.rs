//! SQL executor

use super::{parse_sql, ExecuteResult, expr::{eval_expr, eval_expr_join, JoinContext}, Session, Aggregator};
use super::helpers::{
    dedup_rows, value_to_sql_expr, parse_value_for_copy,
    collect_having_agg_funcs, eval_having_expr, eval_having_expr_join,
    get_skip_reason, get_unsupported_reason,
    get_select_item_name, fill_row_defaults, eval_default_expr,
};
use super::window::{extract_window_functions, compute_window_functions};
use super::ddl;
use super::rbac;
use super::dml;
use super::query;
use super::planner::{self, ScanType};
use crate::auth::AuthManager;
use crate::storage::TikvStore;
use crate::types::{ColumnDef, DataType, Row, TableSchema, Value};
use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AlterTableOperation, AlterRoleOperation, Assignment, ColumnDef as SqlColumnDef,
    Expr, Ident, ObjectName, Query, SelectItem, SetExpr, Statement, 
    TableConstraint, Values, FunctionArg, FunctionArgExpr, OrderByExpr, BinaryOperator, GroupByExpr, 
    JoinOperator, JoinConstraint, TableFactor, LockType, Password as SqlPassword,
    Value as SqlValue, SetOperator, SetQuantifier, OnInsert,
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

    // --- DDL methods (delegated to ddl module) ---

    async fn execute_create_table(&self, txn: &mut Transaction, name: &ObjectName, columns: &[SqlColumnDef], constraints: &[TableConstraint], if_not_exists: bool) -> Result<ExecuteResult> {
        ddl::execute_create_table(&self.store, txn, name, columns, constraints, if_not_exists).await
    }

    async fn execute_create_table_as(&self, txn: &mut Transaction, name: &ObjectName, query: &Query, columns: &[SqlColumnDef], if_not_exists: bool, _temporary: bool) -> Result<ExecuteResult> {
        let table_name = name.0.last().ok_or_else(|| anyhow!("Invalid table name"))?.value.clone();
        
        let ctes = self.build_cte_context(txn, query).await?;
        let result = self.execute_query_with_ctes(txn, query, &ctes).await?;
        
        let (result_cols, result_rows) = match result {
            ExecuteResult::Select { columns: cols, rows } => (cols, rows),
            _ => return Err(anyhow!("CREATE TABLE AS requires a SELECT query")),
        };
        
        ddl::create_table_from_query_result(&self.store, txn, &table_name, if_not_exists, result_cols, result_rows, columns).await
    }

    async fn create_table_from_result(&self, txn: &mut Transaction, target_name: &ObjectName, result: ExecuteResult) -> Result<ExecuteResult> {
        let table_name = target_name.0.last().ok_or_else(|| anyhow!("Invalid table name"))?.value.clone();
        
        let (result_cols, result_rows) = match result {
            ExecuteResult::Select { columns: cols, rows } => (cols, rows),
            _ => return Err(anyhow!("SELECT INTO requires a SELECT query")),
        };
        
        ddl::create_table_from_select_into(&self.store, txn, &table_name, result_cols, result_rows).await
    }

    async fn execute_create_index(&self, txn: &mut Transaction, idx_name: &str, table_name: &ObjectName, columns: &[OrderByExpr], unique: bool, if_not_exists: bool) -> Result<ExecuteResult> {
        let tbl_name = table_name.0.last().unwrap().value.clone();
        let schema = self.store.get_schema(txn, &tbl_name).await?.ok_or_else(|| anyhow!("Table not found"))?;
        let rows = self.scan_and_fill(txn, &tbl_name, &schema).await?;
        ddl::execute_create_index(&self.store, txn, idx_name, table_name, columns, unique, if_not_exists, rows).await
    }

    async fn execute_create_view(&self, txn: &mut Transaction, name: &ObjectName, query: &Query, or_replace: bool) -> Result<ExecuteResult> {
        ddl::execute_create_view(&self.store, txn, name, query, or_replace).await
    }

    async fn execute_drop_view(&self, txn: &mut Transaction, names: &[ObjectName], if_exists: bool) -> Result<ExecuteResult> {
        ddl::execute_drop_view(&self.store, txn, names, if_exists).await
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
                
                let rows = self.scan_and_fill(txn, &table_name, &schema).await?;
                if let Some(dropped) = ddl::execute_drop_index(&self.store, txn, &idx_name, &mut schema, &table_name, rows).await? {
                    found = true;
                    last_index = dropped;
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
        ddl::execute_drop_table(&self.store, txn, names, if_exists).await
    }

    async fn execute_truncate(&self, txn: &mut Transaction, table_name: &ObjectName) -> Result<ExecuteResult> {
        ddl::execute_truncate(&self.store, txn, table_name).await
    }

    async fn execute_alter_table(&self, txn: &mut Transaction, name: &ObjectName, operation: &AlterTableOperation) -> Result<ExecuteResult> {
        let t = name.0.last().unwrap().value.clone();
        let schema = self.store.get_schema(txn, &t).await?.ok_or_else(|| anyhow!("Table '{}' does not exist", t))?;
        let rows = self.scan_and_fill(txn, &t, &schema).await?;
        ddl::execute_alter_table(&self.store, txn, name, operation, rows).await
    }

    async fn execute_insert(&self, txn: &mut Transaction, table_name: &ObjectName, columns: &[Ident], source: &Option<Box<Query>>, returning: &Option<Vec<SelectItem>>, on_conflict: &Option<OnInsert>) -> Result<ExecuteResult> {
        let t = table_name.0.last().unwrap().value.clone();
        let schema = self.store.get_schema(txn, &t).await?.ok_or_else(|| anyhow!("Table '{}' does not exist", t))?;
        let source = source.as_ref().ok_or_else(|| anyhow!("INSERT requires VALUES"))?;
        let values = match &*source.body { SetExpr::Values(Values { rows, .. }) => rows, _ => return Err(anyhow!("Only VALUES supported")) };
        
        let mut affected = 0;
        let mut ret_rows = Vec::new();
        let ret_cols = dml::build_returning_columns(returning, &schema)?;
        
        for exprs in values {
            let (mut row_vals, indices) = dml::prepare_insert_row(&schema, columns, exprs)?;
            dml::fill_missing_columns(&self.store, txn, &schema, &mut row_vals, &indices).await?;
            dml::coerce_row_values(&schema, &mut row_vals)?;
            let row = Row::new(row_vals);
            
            let result = dml::execute_insert_row(&self.store, txn, &t, &schema, row, on_conflict).await?;
            if let Some(final_row) = result {
                affected += 1;
                if let Some(ret_row) = dml::eval_returning_row(returning, &final_row, &schema)? {
                    ret_rows.push(ret_row);
                }
            }
        }
        
        if returning.is_some() { 
            Ok(ExecuteResult::Select { columns: ret_cols, rows: ret_rows }) 
        } else { 
            Ok(ExecuteResult::Insert { affected_rows: affected }) 
        }
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
        let ret_cols = dml::build_returning_columns(returning, &schema)?;
        
        for r in rows {
            if let Some(ref e) = resolved_selection {
                if !matches!(eval_expr(e, Some(&r), Some(&schema))?, Value::Boolean(true)) { continue; }
            }
            if let Some(ret_row) = dml::eval_returning_row(returning, &r, &schema)? {
                ret_rows.push(ret_row);
            }
            dml::execute_delete_row(&self.store, txn, &t, &schema, &r).await?;
            cnt += 1;
        }
        
        if returning.is_some() { 
            Ok(ExecuteResult::Select { columns: ret_cols, rows: ret_rows }) 
        } else { 
            Ok(ExecuteResult::Delete { affected_rows: cnt }) 
        }
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
        let indices = dml::validate_update_columns(&schema, assignments)?;
        
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
        let ret_cols = dml::build_returning_columns(returning, &schema)?;
        
        for r in &rows {
            let matching_from_rows: Vec<&Row> = if let (Some(ref fs), Some(ref fr), Some(ref fa)) = (&from_schema, &from_rows, &from_alias) {
                let (combined_schema, _, column_offsets) = dml::build_update_join_context(&schema, &table_alias, fs, fa, r, &fr[0]);
                
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
            
            let new_vals = if let (Some(ref fs), Some(ref fa)) = (&from_schema, &from_alias) {
                if let Some(first_from) = matching_from_rows.first() {
                    let (combined_schema, combined_row, _) = dml::build_update_join_context(&schema, &table_alias, fs, fa, r, first_from);
                    dml::compute_update_values(&schema, r, assignments, &indices, Some((&combined_row, &combined_schema)))?
                } else {
                    dml::compute_update_values(&schema, r, assignments, &indices, None)?
                }
            } else {
                dml::compute_update_values(&schema, r, assignments, &indices, None)?
            };
            
            let new_row = Row::new(new_vals);
            let updated_row = dml::execute_update_row(&self.store, txn, &t, &schema, r, new_row).await?;
            
            if let Some(ret_row) = dml::eval_returning_row(returning, &updated_row, &schema)? {
                ret_rows.push(ret_row);
            }
            cnt += 1;
        }
        
        if returning.is_some() { 
            Ok(ExecuteResult::Select { columns: ret_cols, rows: ret_rows }) 
        } else { 
            Ok(ExecuteResult::Update { affected_rows: cnt }) 
        }
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
                let predicates = planner::analyze_predicates(sel);
                let estimated_rows = all_rows_base.len().max(100);
                let access_path = planner::choose_best_access_path(&schema, &predicates, estimated_rows);
                
                match access_path.scan_type {
                    ScanType::IndexScan { index_id, ref index_name, ref values, .. } => {
                        let index = schema.indexes.iter().find(|i| i.id == index_id);
                        if let Some(idx) = index {
                            debug!("Using Index Scan on {} (cost: {:.2})", index_name, access_path.cost);
                            let pks = self.store.scan_index(txn, schema.table_id, idx.id, values, idx.unique).await?;
                            if !pks.is_empty() {
                                let mut rows = self.store.batch_get_rows(txn, schema.table_id, pks.clone(), &schema).await?;
                                if !rows.is_empty() {
                                    for r in &mut rows { fill_row_defaults(r, &schema)?; }
                                    index_scan_rows = Some(rows);
                                }
                            }
                        }
                    }
                    ScanType::IndexRangeScan { index_id, ref index_name, ref prefix_values, .. } => {
                        let index = schema.indexes.iter().find(|i| i.id == index_id);
                        if let Some(idx) = index {
                            debug!("Using Index Range Scan on {} with {} prefix columns (cost: {:.2})", 
                                   index_name, prefix_values.len(), access_path.cost);
                            let pks = self.store.scan_index(txn, schema.table_id, idx.id, prefix_values, idx.unique).await?;
                            if !pks.is_empty() {
                                let mut rows = self.store.batch_get_rows(txn, schema.table_id, pks.clone(), &schema).await?;
                                if !rows.is_empty() {
                                    for r in &mut rows { fill_row_defaults(r, &schema)?; }
                                    index_scan_rows = Some(rows);
                                }
                            }
                        }
                    }
                    ScanType::FullTableScan => {
                        debug!("Using Full Table Scan (cost: {:.2})", access_path.cost);
                    }
                }
            }
            index_scan_rows.unwrap_or(all_rows_base)
        };
        
        let filtered_rows = if let Some(ref sel) = resolved_selection {
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

            let is_all = query::is_set_quantifier_all(quantifier);
            let rows = match op {
                SetOperator::Union => query::apply_union(left_rows, right_rows, is_all),
                SetOperator::Intersect => query::apply_intersect(left_rows, right_rows, is_all),
                SetOperator::Except => query::apply_except(left_rows, right_rows, is_all),
            };

            Ok(ExecuteResult::Select { columns: left_cols, rows })
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
        parse_value_for_copy(val, data_type)
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
        rbac::execute_create_role(&self.auth_manager, txn, names, if_not_exists, login, password, superuser, create_db, create_role).await
    }

    async fn execute_alter_role(
        &self,
        txn: &mut Transaction,
        name: &Ident,
        operation: &AlterRoleOperation,
    ) -> Result<ExecuteResult> {
        rbac::execute_alter_role(&self.auth_manager, txn, name, operation).await
    }

    async fn execute_drop_role(
        &self,
        txn: &mut Transaction,
        names: &[ObjectName],
        if_exists: bool,
    ) -> Result<ExecuteResult> {
        rbac::execute_drop_role(&self.auth_manager, txn, names, if_exists).await
    }

    async fn execute_grant(
        &self,
        txn: &mut Transaction,
        privileges: &Privileges,
        objects: &Option<GrantObjects>,
        grantees: &[Ident],
        with_grant_option: bool,
    ) -> Result<ExecuteResult> {
        rbac::execute_grant(&self.auth_manager, txn, privileges, objects, grantees, with_grant_option).await
    }

    async fn execute_revoke(
        &self,
        txn: &mut Transaction,
        privileges: &Privileges,
        objects: &Option<GrantObjects>,
        grantees: &[Ident],
    ) -> Result<ExecuteResult> {
        rbac::execute_revoke(&self.auth_manager, txn, privileges, objects, grantees).await
    }
}
