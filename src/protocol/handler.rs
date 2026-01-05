//! PostgreSQL protocol handler using pgwire

use crate::sql::{ExecuteResult, Executor, Session};
use crate::storage::TikvStore;
use crate::types::Value;
use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use pgwire::api::auth::{ServerParameterProvider, StartupHandler};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireConnectionState, PgWireServerHandlers, Type, METADATA_USER};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::data::DataRow;
use pgwire::messages::response::{CommandComplete, ErrorResponse};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use std::collections::HashMap;

use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, error, info, warn};

/// Custom metadata key for storing the extracted keyspace
const METADATA_KEYSPACE: &str = "keyspace";
/// Custom metadata key for storing the actual username (after parsing tenant.user)
const METADATA_ACTUAL_USER: &str = "actual_user";

pub struct PgServerParameterProvider;

impl ServerParameterProvider for PgServerParameterProvider {
    fn server_parameters<C: ClientInfo>(&self, _client: &C) -> Option<HashMap<String, String>> {
        let mut params = HashMap::new();
        params.insert("server_version".to_owned(), "15.0".to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO, MDY".to_owned());
        Some(params)
    }
}

#[derive(Debug, Clone)]
pub struct CopyContext {
    pub table_name: String,
    pub columns: Vec<String>,
    pub data_buffer: Vec<Vec<u8>>,
}

/// Parse username in format "tenant.user" or "tenant:user" into (keyspace, actual_user).
/// If no separator found, returns (None, username) - no keyspace override.
fn parse_tenant_username(username: &str) -> (Option<String>, String) {
    // Try dot separator first: "tenant_a.admin" -> keyspace=tenant_a, user=admin
    if let Some(pos) = username.find('.') {
        let tenant = &username[..pos];
        let user = &username[pos + 1..];
        if !tenant.is_empty() && !user.is_empty() {
            return (Some(tenant.to_string()), user.to_string());
        }
    }
    // Try colon separator: "tenant_a:admin" -> keyspace=tenant_a, user=admin  
    if let Some(pos) = username.find(':') {
        let tenant = &username[..pos];
        let user = &username[pos + 1..];
        if !tenant.is_empty() && !user.is_empty() {
            return (Some(tenant.to_string()), user.to_string());
        }
    }
    // No separator or invalid format - use as-is without keyspace override
    (None, username.to_string())
}

/// Dynamic handler that creates executor lazily after authentication
pub struct DynamicPgHandler {
    pd_endpoints: Vec<String>,
    namespace: Option<String>,
    default_keyspace: Option<String>,
    expected_password: Option<String>,
    executor: OnceCell<Arc<Executor>>,
    session: Mutex<Option<Session>>,
    copy_context: Mutex<Option<CopyContext>>,
    query_parser: Arc<NoopQueryParser>,
}

impl DynamicPgHandler {
    pub fn new(
        pd_endpoints: Vec<String>,
        namespace: Option<String>,
        default_keyspace: Option<String>,
        expected_password: Option<String>,
    ) -> Self {
        Self {
            pd_endpoints,
            namespace,
            default_keyspace,
            expected_password,
            executor: OnceCell::new(),
            session: Mutex::new(None),
            copy_context: Mutex::new(None),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    async fn init_executor(&self, keyspace: Option<String>) -> Result<(), String> {
        let effective_keyspace = keyspace
            .or_else(|| self.default_keyspace.clone())
            .or_else(|| Some("default".to_string()));
        
        match TikvStore::new_with_keyspace(
            self.pd_endpoints.clone(),
            self.namespace.clone(),
            effective_keyspace.clone(),
        ).await {
            Ok(store) => {
                let store = Arc::new(store);
                let executor = Arc::new(Executor::new(store.clone()));
                let session = Session::new(store);
                
                // Set executor (ignore if already set - race condition)
                let _ = self.executor.set(executor);
                
                // Set session
                let mut session_guard = self.session.lock().await;
                *session_guard = Some(session);
                
                info!("Initialized executor with keyspace: {:?}", effective_keyspace);
                Ok(())
            }
            Err(e) => {
                error!("Failed to create TikvStore: {}", e);
                Err(format!("Failed to connect to TiKV: {}", e))
            }
        }
    }

    fn get_executor(&self) -> Result<&Arc<Executor>, PgWireError> {
        self.executor.get().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "XX000".to_string(),
                "Executor not initialized - authentication required".to_string(),
            )))
        })
    }

    fn parse_copy_command(query: &str) -> Option<(String, Vec<String>)> {
        let query_upper = query.to_uppercase();
        if !query_upper.contains("COPY") || !query_upper.contains("FROM") || !query_upper.contains("STDIN") {
            return None;
        }

        // Regex: COPY [public.]table_name (col1, col2, ...) FROM stdin
        let re = regex::Regex::new(r"(?i)COPY\s+(?:public\.)?(\w+)\s*\(([^)]+)\)\s+FROM\s+stdin").ok()?;
        if let Some(caps) = re.captures(query) {
            let table_name = caps.get(1)?.as_str().to_string();
            let columns_str = caps.get(2)?.as_str();
            let columns: Vec<String> = columns_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            return Some((table_name, columns));
        }

        // Regex: COPY [public.]table_name FROM stdin (no column list)
        let re2 = regex::Regex::new(r"(?i)COPY\s+(?:public\.)?(\w+)\s+FROM\s+stdin").ok()?;
        if let Some(caps) = re2.captures(query) {
            let table_name = caps.get(1)?.as_str().to_string();
            return Some((table_name, vec![]));
        }

        None
    }
}

#[async_trait]
impl StartupHandler for DynamicPgHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                // Save startup parameters to metadata
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                
                // Parse username to extract keyspace
                if let Some(raw_user) = client.metadata().get(METADATA_USER).cloned() {
                    let (keyspace, actual_user) = parse_tenant_username(&raw_user);
                    
                    if let Some(ks) = &keyspace {
                        client.metadata_mut().insert(METADATA_KEYSPACE.to_string(), ks.clone());
                        info!("Extracted keyspace '{}' from username '{}'", ks, raw_user);
                    }
                    client.metadata_mut().insert(METADATA_ACTUAL_USER.to_string(), actual_user.clone());
                    info!("Actual user: {}", actual_user);
                }
                
                // Check if password auth is required
                if self.expected_password.is_some() {
                    // Request password
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    // No password required - initialize executor and finish auth
                    let keyspace = client.metadata().get(METADATA_KEYSPACE).cloned();
                    
                    if let Err(e) = self.init_executor(keyspace).await {
                        let error_info = ErrorInfo::new(
                            "FATAL".to_owned(),
                            "XX000".to_owned(),
                            e,
                        );
                        client.feed(PgWireBackendMessage::ErrorResponse(ErrorResponse::from(error_info))).await?;
                        client.close().await?;
                        return Ok(());
                    }
                    
                    pgwire::api::auth::finish_authentication(client, &PgServerParameterProvider).await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let provided_password = pwd.password.clone();
                
                // Validate password
                let password_valid = match &self.expected_password {
                    Some(expected) => provided_password == *expected,
                    None => true,
                };
                
                if password_valid {
                    // Initialize executor with extracted keyspace
                    let keyspace = client.metadata().get(METADATA_KEYSPACE).cloned();
                    
                    if let Err(e) = self.init_executor(keyspace).await {
                        let error_info = ErrorInfo::new(
                            "FATAL".to_owned(),
                            "XX000".to_owned(),
                            e,
                        );
                        client.feed(PgWireBackendMessage::ErrorResponse(ErrorResponse::from(error_info))).await?;
                        client.close().await?;
                        return Ok(());
                    }
                    
                    pgwire::api::auth::finish_authentication(client, &PgServerParameterProvider).await?;
                    
                    let actual_user = client.metadata().get(METADATA_ACTUAL_USER).cloned().unwrap_or_default();
                    let keyspace = client.metadata().get(METADATA_KEYSPACE).cloned();
                    info!("Authentication successful for user '{}' with keyspace {:?}", actual_user, keyspace);
                } else {
                    let error_info = ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28P01".to_owned(),
                        "Password authentication failed".to_owned(),
                    );
                    client.feed(PgWireBackendMessage::ErrorResponse(ErrorResponse::from(error_info))).await?;
                    client.close().await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for DynamicPgHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!("Received query: {}", query);

        let executor = self.get_executor()?;

        if let Some((table_name, columns)) = Self::parse_copy_command(query) {
            info!("COPY command detected: table={}, columns={:?}", table_name, columns);
            
            let col_count = if columns.is_empty() {
                let mut session_guard = self.session.lock().await;
                let session = session_guard.as_mut().ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(), "XX000".to_string(), "Session not initialized".to_string(),
                    )))
                })?;
                session.begin().await.ok();
                let count = if let Some(txn) = session.get_mut_txn() {
                    if let Ok(Some(schema)) = executor.store().get_schema(txn, &table_name).await {
                        schema.columns.len()
                    } else {
                        1
                    }
                } else {
                    1
                };
                session.rollback().await.ok();
                count
            } else {
                columns.len()
            };
            
            let mut ctx = self.copy_context.lock().await;
            *ctx = Some(CopyContext {
                table_name,
                columns,
                data_buffer: Vec::new(),
            });
            
            let column_formats: Vec<i16> = vec![0; col_count];
            return Ok(vec![Response::CopyIn(CopyResponse::new(0, col_count, column_formats))]);
        }

        let mut session_guard = self.session.lock().await;
        let session = session_guard.as_mut().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(), "XX000".to_string(), "Session not initialized".to_string(),
            )))
        })?;

        match executor.execute(session, query).await {
            Ok(result) => {
                let response = result_to_response(result)?;
                Ok(vec![response])
            }
            Err(e) => {
                error!("Query execution error: {}", e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "XX000".to_string(),
                    e.to_string(),
                ))))
            }
        }
    }
}

#[async_trait]
impl CopyHandler for DynamicPgHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut ctx_guard = self.copy_context.lock().await;
        if let Some(ref mut ctx) = *ctx_guard {
            ctx.data_buffer.push(copy_data.data.to_vec());
        }
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let executor = self.get_executor()?;
        
        let ctx_opt = {
            let mut ctx_guard = self.copy_context.lock().await;
            ctx_guard.take()
        };

        let row_count = if let Some(ctx) = ctx_opt {
            info!("COPY done for table {}, processing {} data chunks", ctx.table_name, ctx.data_buffer.len());
            
            let mut session_guard = self.session.lock().await;
            let session = session_guard.as_mut().ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(), "XX000".to_string(), "Session not initialized".to_string(),
                )))
            })?;
            
            session.begin().await.map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(), "XX000".to_string(), e.to_string(),
            ))))?;

            let schema = {
                let txn = session.get_mut_txn().ok_or_else(|| PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(), "XX000".to_string(), "No transaction".to_string(),
                ))))?;
                executor.store()
                    .get_schema(txn, &ctx.table_name)
                    .await
                    .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(), "XX000".to_string(), e.to_string(),
                    ))))?
                    .ok_or_else(|| PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "42P01".to_string(),
                        format!("relation \"{}\" does not exist", ctx.table_name),
                    ))))?
            };
            session.rollback().await.ok();

            let columns: Vec<String> = if ctx.columns.is_empty() {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                ctx.columns.clone()
            };

            let mut all_data = Vec::new();
            for chunk in &ctx.data_buffer {
                all_data.extend_from_slice(chunk);
            }
            
            let data_str = String::from_utf8_lossy(&all_data);
            let lines: Vec<&str> = data_str.lines().filter(|l| !l.is_empty()).collect();
            
            info!("Processing {} rows for COPY into {}", lines.len(), ctx.table_name);

            let mut count = 0usize;

            for line in lines {
                let values: Vec<&str> = line.split('\t').collect();
                
                if values.len() != columns.len() {
                    warn!("COPY row has {} values but expected {} columns, skipping", values.len(), columns.len());
                    continue;
                }

                let mut col_values: Vec<(String, Value)> = Vec::new();
                for (col_name, val) in columns.iter().zip(values.iter()) {
                    let value = if *val == "\\N" {
                        Value::Null
                    } else {
                        let col_schema = schema.columns.iter().find(|c| c.name == *col_name);
                        if let Some(cs) = col_schema {
                            executor.parse_value_for_copy(val, &cs.data_type)
                        } else {
                            Value::Text(val.to_string())
                        }
                    };
                    col_values.push((col_name.clone(), value));
                }

                if let Err(e) = executor.execute_copy_insert(session, &ctx.table_name, col_values).await {
                    error!("COPY insert error: {}", e);
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(), "XX000".to_string(), e.to_string(),
                    ))));
                }
                count += 1;
            }

            info!("COPY completed: {} rows inserted into {}", count, ctx.table_name);
            count
        } else {
            0
        };

        client.send(PgWireBackendMessage::CommandComplete(
            CommandComplete::new(format!("COPY {}", row_count))
        )).await?;

        Ok(())
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut ctx_guard = self.copy_context.lock().await;
        *ctx_guard = None;

        warn!("COPY failed: {}", fail.message);
        
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("COPY IN mode terminated: {}", fail.message),
        )))
    }
}

#[async_trait]
impl ExtendedQueryHandler for DynamicPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let executor = self.get_executor()?;
        let query = &portal.statement.statement;
        debug!("Extended query: {}", query);

        let final_query = substitute_parameters(query, portal);
        debug!("Final query after substitution: {}", final_query);

        let mut session_guard = self.session.lock().await;
        let session = session_guard.as_mut().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(), "XX000".to_string(), "Session not initialized".to_string(),
            )))
        })?;
        
        match executor.execute(session, &final_query).await {
            Ok(result) => result_to_response(result),
            Err(e) => {
                error!("Extended query execution error: {}", e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "XX000".to_string(),
                    e.to_string(),
                ))))
            }
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let param_types: Vec<Type> = stmt.parameter_types.clone();
        let fields = infer_result_fields(&stmt.statement);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = infer_result_fields(&portal.statement.statement);
        Ok(DescribePortalResponse::new(fields))
    }
}

fn substitute_parameters(query: &str, portal: &Portal<String>) -> String {
    let mut result = query.to_string();
    
    for i in 0..portal.parameter_len() {
        let placeholder = format!("${}", i + 1);
        let param_type = portal
            .statement
            .parameter_types
            .get(i)
            .cloned()
            .unwrap_or(Type::TEXT);
        
        let value_str = match &param_type {
            t if *t == Type::BOOL => {
                portal.parameter::<bool>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| if v { "true" } else { "false" }.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
            t if *t == Type::INT2 => {
                portal.parameter::<i16>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
            t if *t == Type::INT4 => {
                portal.parameter::<i32>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
            t if *t == Type::INT8 => {
                portal.parameter::<i64>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
            t if *t == Type::FLOAT4 => {
                portal.parameter::<f32>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
            t if *t == Type::FLOAT8 => {
                portal.parameter::<f64>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
            _ => {
                portal.parameter::<String>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| format!("'{}'", v.replace("'", "''")))
                    .unwrap_or_else(|| "NULL".to_string())
            }
        };
        
        result = result.replace(&placeholder, &value_str);
    }
    
    result
}

fn infer_result_fields(query: &str) -> Vec<FieldInfo> {
    let query_upper = query.to_uppercase();
    if query_upper.starts_with("SELECT") {
        vec![FieldInfo::new(
            "column".to_string(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )]
    } else {
        vec![]
    }
}

/// Dynamic handler factory that creates per-connection handlers with dynamic keyspace
pub struct DynamicHandlerFactory {
    pd_endpoints: Vec<String>,
    namespace: Option<String>,
    default_keyspace: Option<String>,
    expected_password: Option<String>,
}

impl DynamicHandlerFactory {
    pub fn new(
        pd_endpoints: Vec<String>,
        namespace: Option<String>,
        default_keyspace: Option<String>,
        expected_password: Option<String>,
    ) -> Self {
        Self {
            pd_endpoints,
            namespace,
            default_keyspace,
            expected_password,
        }
    }
}

impl PgWireServerHandlers for DynamicHandlerFactory {
    type StartupHandler = DynamicPgHandler;
    type SimpleQueryHandler = DynamicPgHandler;
    type ExtendedQueryHandler = DynamicPgHandler;
    type CopyHandler = DynamicPgHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(DynamicPgHandler::new(
            self.pd_endpoints.clone(),
            self.namespace.clone(),
            self.default_keyspace.clone(),
            self.expected_password.clone(),
        ))
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(DynamicPgHandler::new(
            self.pd_endpoints.clone(),
            self.namespace.clone(),
            self.default_keyspace.clone(),
            self.expected_password.clone(),
        ))
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(DynamicPgHandler::new(
            self.pd_endpoints.clone(),
            self.namespace.clone(),
            self.default_keyspace.clone(),
            self.expected_password.clone(),
        ))
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(DynamicPgHandler::new(
            self.pd_endpoints.clone(),
            self.namespace.clone(),
            self.default_keyspace.clone(),
            self.expected_password.clone(),
        ))
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

// Keep the old HandlerFactory for backward compatibility (static executor)
pub struct HandlerFactory {
    handler: Arc<PgHandler>,
}

impl HandlerFactory {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self { 
            handler: Arc::new(PgHandler::new(executor)),
        }
    }
}

pub struct PgHandler {
    executor: Arc<Executor>,
    session: Mutex<Session>,
    copy_context: Mutex<Option<CopyContext>>,
    query_parser: Arc<NoopQueryParser>,
}

impl PgHandler {
    pub fn new(executor: Arc<Executor>) -> Self {
        let store = executor.store();
        Self {
            executor,
            session: Mutex::new(Session::new(store)),
            copy_context: Mutex::new(None),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    fn parse_copy_command(query: &str) -> Option<(String, Vec<String>)> {
        let query_upper = query.to_uppercase();
        if !query_upper.contains("COPY") || !query_upper.contains("FROM") || !query_upper.contains("STDIN") {
            return None;
        }

        let re = regex::Regex::new(r"(?i)COPY\s+(?:public\.)?(\w+)\s*\(([^)]+)\)\s+FROM\s+stdin").ok()?;
        if let Some(caps) = re.captures(query) {
            let table_name = caps.get(1)?.as_str().to_string();
            let columns_str = caps.get(2)?.as_str();
            let columns: Vec<String> = columns_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            return Some((table_name, columns));
        }

        let re2 = regex::Regex::new(r"(?i)COPY\s+(?:public\.)?(\w+)\s+FROM\s+stdin").ok()?;
        if let Some(caps) = re2.captures(query) {
            let table_name = caps.get(1)?.as_str().to_string();
            return Some((table_name, vec![]));
        }

        None
    }
}

#[async_trait]
impl StartupHandler for PgHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
            pgwire::api::auth::finish_authentication(client, &PgServerParameterProvider).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for PgHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!("Received query: {}", query);

        if let Some((table_name, columns)) = Self::parse_copy_command(query) {
            info!("COPY command detected: table={}, columns={:?}", table_name, columns);
            
            let col_count = if columns.is_empty() {
                let mut session = self.session.lock().await;
                session.begin().await.ok();
                let count = if let Some(txn) = session.get_mut_txn() {
                    if let Ok(Some(schema)) = self.executor.store().get_schema(txn, &table_name).await {
                        schema.columns.len()
                    } else {
                        1
                    }
                } else {
                    1
                };
                session.rollback().await.ok();
                count
            } else {
                columns.len()
            };
            
            let mut ctx = self.copy_context.lock().await;
            *ctx = Some(CopyContext {
                table_name,
                columns,
                data_buffer: Vec::new(),
            });
            
            let column_formats: Vec<i16> = vec![0; col_count];
            return Ok(vec![Response::CopyIn(CopyResponse::new(0, col_count, column_formats))]);
        }

        let mut session = self.session.lock().await;

        match self.executor.execute(&mut session, query).await {
            Ok(result) => {
                let response = result_to_response(result)?;
                Ok(vec![response])
            }
            Err(e) => {
                error!("Query execution error: {}", e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "XX000".to_string(),
                    e.to_string(),
                ))))
            }
        }
    }
}

#[async_trait]
impl CopyHandler for PgHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut ctx_guard = self.copy_context.lock().await;
        if let Some(ref mut ctx) = *ctx_guard {
            ctx.data_buffer.push(copy_data.data.to_vec());
        }
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let ctx_opt = {
            let mut ctx_guard = self.copy_context.lock().await;
            ctx_guard.take()
        };

        let row_count = if let Some(ctx) = ctx_opt {
            info!("COPY done for table {}, processing {} data chunks", ctx.table_name, ctx.data_buffer.len());
            
            let mut session = self.session.lock().await;
            session.begin().await.map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(), "XX000".to_string(), e.to_string(),
            ))))?;

            let schema = {
                let txn = session.get_mut_txn().ok_or_else(|| PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(), "XX000".to_string(), "No transaction".to_string(),
                ))))?;
                self.executor.store()
                    .get_schema(txn, &ctx.table_name)
                    .await
                    .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(), "XX000".to_string(), e.to_string(),
                    ))))?
                    .ok_or_else(|| PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "42P01".to_string(),
                        format!("relation \"{}\" does not exist", ctx.table_name),
                    ))))?
            };
            session.rollback().await.ok();

            let columns: Vec<String> = if ctx.columns.is_empty() {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                ctx.columns.clone()
            };

            let mut all_data = Vec::new();
            for chunk in &ctx.data_buffer {
                all_data.extend_from_slice(chunk);
            }
            
            let data_str = String::from_utf8_lossy(&all_data);
            let lines: Vec<&str> = data_str.lines().filter(|l| !l.is_empty()).collect();
            
            info!("Processing {} rows for COPY into {}", lines.len(), ctx.table_name);

            let mut count = 0usize;

            for line in lines {
                let values: Vec<&str> = line.split('\t').collect();
                
                if values.len() != columns.len() {
                    warn!("COPY row has {} values but expected {} columns, skipping", values.len(), columns.len());
                    continue;
                }

                let mut col_values: Vec<(String, Value)> = Vec::new();
                for (col_name, val) in columns.iter().zip(values.iter()) {
                    let value = if *val == "\\N" {
                        Value::Null
                    } else {
                        let col_schema = schema.columns.iter().find(|c| c.name == *col_name);
                        if let Some(cs) = col_schema {
                            self.executor.parse_value_for_copy(val, &cs.data_type)
                        } else {
                            Value::Text(val.to_string())
                        }
                    };
                    col_values.push((col_name.clone(), value));
                }

                if let Err(e) = self.executor.execute_copy_insert(&mut session, &ctx.table_name, col_values).await {
                    error!("COPY insert error: {}", e);
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(), "XX000".to_string(), e.to_string(),
                    ))));
                }
                count += 1;
            }

            info!("COPY completed: {} rows inserted into {}", count, ctx.table_name);
            count
        } else {
            0
        };

        client.send(PgWireBackendMessage::CommandComplete(
            CommandComplete::new(format!("COPY {}", row_count))
        )).await?;

        Ok(())
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut ctx_guard = self.copy_context.lock().await;
        *ctx_guard = None;

        warn!("COPY failed: {}", fail.message);
        
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("COPY IN mode terminated: {}", fail.message),
        )))
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        debug!("Extended query: {}", query);

        let final_query = substitute_parameters(query, portal);
        debug!("Final query after substitution: {}", final_query);

        let mut session = self.session.lock().await;
        match self.executor.execute(&mut session, &final_query).await {
            Ok(result) => result_to_response(result),
            Err(e) => {
                error!("Extended query execution error: {}", e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "XX000".to_string(),
                    e.to_string(),
                ))))
            }
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let param_types: Vec<Type> = stmt.parameter_types.clone();
        let fields = infer_result_fields(&stmt.statement);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = infer_result_fields(&portal.statement.statement);
        Ok(DescribePortalResponse::new(fields))
    }
}

impl PgWireServerHandlers for HandlerFactory {
    type StartupHandler = PgHandler;
    type SimpleQueryHandler = PgHandler;
    type ExtendedQueryHandler = PgHandler;
    type CopyHandler = PgHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.handler.clone()
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

fn result_to_response(result: ExecuteResult) -> PgWireResult<Response<'static>> {
    match result {
        ExecuteResult::Select { columns, rows } => {
            let fields: Vec<FieldInfo> = columns
                .iter()
                .map(|name| {
                    FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text)
                })
                .collect();

            let fields = Arc::new(fields);
            
            let mut data_rows: Vec<PgWireResult<DataRow>> = Vec::new();
            for row in rows {
                let mut encoder = DataRowEncoder::new(fields.clone());
                for value in &row.values {
                    encode_value(&mut encoder, value)?;
                }
                data_rows.push(encoder.finish());
            }

            let row_stream = stream::iter(data_rows);
            let results = QueryResponse::new(fields, row_stream);

            Ok(Response::Query(results))
        }

        ExecuteResult::CreateTable { .. } => {
            Ok(Response::Execution(Tag::new("CREATE TABLE")))
        }

        ExecuteResult::DropTable { .. } => {
            Ok(Response::Execution(Tag::new("DROP TABLE")))
        }

        ExecuteResult::TruncateTable { .. } => {
            Ok(Response::Execution(Tag::new("TRUNCATE TABLE")))
        }

        ExecuteResult::CreateIndex { .. } => {
            Ok(Response::Execution(Tag::new("CREATE INDEX")))
        }

        ExecuteResult::DropIndex { .. } => {
            Ok(Response::Execution(Tag::new("DROP INDEX")))
        }

        ExecuteResult::CreateView { .. } => {
            Ok(Response::Execution(Tag::new("CREATE VIEW")))
        }

        ExecuteResult::DropView { .. } => {
            Ok(Response::Execution(Tag::new("DROP VIEW")))
        }

        ExecuteResult::AlterTable { .. } => {
            Ok(Response::Execution(Tag::new("ALTER TABLE")))
        }

        ExecuteResult::Insert { affected_rows } => {
            Ok(Response::Execution(Tag::new("INSERT").with_rows(affected_rows as usize)))
        }

        ExecuteResult::Delete { affected_rows } => {
            Ok(Response::Execution(Tag::new("DELETE").with_rows(affected_rows as usize)))
        }

        ExecuteResult::Update { affected_rows } => {
            Ok(Response::Execution(Tag::new("UPDATE").with_rows(affected_rows as usize)))
        }

        ExecuteResult::ShowTables { tables } => {
            let fields = vec![FieldInfo::new(
                "table_name".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )];
            let fields = Arc::new(fields);

            let mut data_rows: Vec<PgWireResult<DataRow>> = Vec::new();
            for table in tables {
                let mut encoder = DataRowEncoder::new(fields.clone());
                encoder.encode_field(&table)?;
                data_rows.push(encoder.finish());
            }

            let row_stream = stream::iter(data_rows);
            let results = QueryResponse::new(fields, row_stream);

            Ok(Response::Query(results))
        }

        ExecuteResult::Describe { schema } => {
            let fields = vec![
                FieldInfo::new("column_name".to_string(), None, None, Type::TEXT, FieldFormat::Text),
                FieldInfo::new("data_type".to_string(), None, None, Type::TEXT, FieldFormat::Text),
                FieldInfo::new("nullable".to_string(), None, None, Type::BOOL, FieldFormat::Text),
                FieldInfo::new("primary_key".to_string(), None, None, Type::BOOL, FieldFormat::Text),
                FieldInfo::new("default".to_string(), None, None, Type::TEXT, FieldFormat::Text),
            ];
            let fields = Arc::new(fields);

            let mut data_rows: Vec<PgWireResult<DataRow>> = Vec::new();
            for col in &schema.columns {
                let mut encoder = DataRowEncoder::new(fields.clone());
                encoder.encode_field(&col.name)?;
                encoder.encode_field(&col.data_type.to_string())?;
                encoder.encode_field(&col.nullable)?;
                encoder.encode_field(&col.primary_key)?;
                
                let default_val = if col.is_serial {
                    Some("SERIAL (AUTO_INC)".to_string())
                } else {
                    col.default_expr.clone()
                };
                encoder.encode_field(&default_val)?;
                
                data_rows.push(encoder.finish());
            }

            let row_stream = stream::iter(data_rows);
            let results = QueryResponse::new(fields, row_stream);

            Ok(Response::Query(results))
        }

        ExecuteResult::Empty => {
            Ok(Response::EmptyQuery)
        }

        ExecuteResult::Skipped { message } => {
            tracing::warn!("SKIPPED: {}", message);
            let fields = vec![FieldInfo::new("warning".to_string(), None, None, Type::TEXT, FieldFormat::Text)];
            let fields = Arc::new(fields);
            let mut encoder = DataRowEncoder::new(fields.clone());
            encoder.encode_field(&format!("SKIPPED: {}", message))?;
            let data_rows = vec![encoder.finish()];
            let row_stream = stream::iter(data_rows);
            Ok(Response::Query(QueryResponse::new(fields, row_stream)))
        }
    }
}

fn encode_value(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&None::<String>),
        Value::Boolean(b) => encoder.encode_field(b),
        Value::Int32(i) => encoder.encode_field(&i.to_string()),
        Value::Int64(i) => encoder.encode_field(&i.to_string()),
        Value::Float64(f) => encoder.encode_field(&f.to_string()),
        Value::Text(s) => encoder.encode_field(s),
        Value::Bytes(b) => encoder.encode_field(&format!("\\x{}", hex::encode(b))),
        Value::Timestamp(ts) => {
            use chrono::{DateTime, NaiveDateTime, Utc};
            let seconds = ts / 1000;
            let nanos = (ts % 1000) * 1_000_000;
            if let Some(naive) = NaiveDateTime::from_timestamp_opt(seconds, nanos as u32) {
                 let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
                 encoder.encode_field(&dt.to_rfc3339())
            } else {
                 encoder.encode_field(&ts.to_string())
            }
        },
        Value::Interval(ms) => {
            let days = *ms / (1000 * 60 * 60 * 24);
            let remaining = *ms % (1000 * 60 * 60 * 24);
            let hours = remaining / (1000 * 60 * 60);
            let remaining = remaining % (1000 * 60 * 60);
            let mins = remaining / (1000 * 60);
            let secs = (remaining % (1000 * 60)) / 1000;
            if days != 0 {
                encoder.encode_field(&format!("{} days {:02}:{:02}:{:02}", days, hours, mins, secs))
            } else {
                encoder.encode_field(&format!("{:02}:{:02}:{:02}", hours, mins, secs))
            }
        },
        Value::Uuid(bytes) => {
            let uuid = uuid::Uuid::from_bytes(*bytes);
            encoder.encode_field(&uuid.to_string())
        }
        Value::Array(elems) => {
            let mut parts = Vec::new();
            for elem in elems {
                match elem {
                    Value::Null => parts.push("NULL".to_string()),
                    Value::Text(s) => parts.push(format!("\"{}\"", s.replace('"', "\\\""))),
                    v => parts.push(v.to_string()),
                }
            }
            encoder.encode_field(&format!("{{{}}}", parts.join(",")))
        }
        Value::Json(s) => encoder.encode_field(s),
        Value::Jsonb(s) => encoder.encode_field(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tenant_username_dot() {
        let (ks, user) = parse_tenant_username("tenant_a.admin");
        assert_eq!(ks, Some("tenant_a".to_string()));
        assert_eq!(user, "admin");
    }

    #[test]
    fn test_parse_tenant_username_colon() {
        let (ks, user) = parse_tenant_username("tenant_b:postgres");
        assert_eq!(ks, Some("tenant_b".to_string()));
        assert_eq!(user, "postgres");
    }

    #[test]
    fn test_parse_tenant_username_no_separator() {
        let (ks, user) = parse_tenant_username("admin");
        assert_eq!(ks, None);
        assert_eq!(user, "admin");
    }

    #[test]
    fn test_parse_tenant_username_empty_parts() {
        let (ks, user) = parse_tenant_username(".admin");
        assert_eq!(ks, None);
        assert_eq!(user, ".admin");

        let (ks, user) = parse_tenant_username("tenant.");
        assert_eq!(ks, None);
        assert_eq!(user, "tenant.");
    }

    #[test]
    fn test_parse_tenant_username_multiple_dots() {
        let (ks, user) = parse_tenant_username("prod.tenant_a.admin");
        assert_eq!(ks, Some("prod".to_string()));
        assert_eq!(user, "tenant_a.admin");
    }

    #[test]
    fn test_parse_tenant_username_multiple_colons() {
        let (ks, user) = parse_tenant_username("prod:tenant_a:admin");
        assert_eq!(ks, Some("prod".to_string()));
        assert_eq!(user, "tenant_a:admin");
    }

    #[test]
    fn test_parse_tenant_username_mixed_separators() {
        let (ks, user) = parse_tenant_username("tenant.user:name");
        assert_eq!(ks, Some("tenant".to_string()));
        assert_eq!(user, "user:name");

        let (ks, user) = parse_tenant_username("tenant:user.name");
        assert_eq!(ks, Some("tenant:user".to_string()));
        assert_eq!(user, "name");
    }

    #[test]
    fn test_parse_tenant_username_special_chars() {
        let (ks, user) = parse_tenant_username("tenant-1.user_name");
        assert_eq!(ks, Some("tenant-1".to_string()));
        assert_eq!(user, "user_name");

        let (ks, user) = parse_tenant_username("my_tenant:pg-admin");
        assert_eq!(ks, Some("my_tenant".to_string()));
        assert_eq!(user, "pg-admin");
    }

    #[test]
    fn test_parse_tenant_username_numbers() {
        let (ks, user) = parse_tenant_username("tenant123.user456");
        assert_eq!(ks, Some("tenant123".to_string()));
        assert_eq!(user, "user456");
    }

    #[test]
    fn test_parse_tenant_username_empty_string() {
        let (ks, user) = parse_tenant_username("");
        assert_eq!(ks, None);
        assert_eq!(user, "");
    }

    #[test]
    fn test_parse_tenant_username_only_separator() {
        let (ks, user) = parse_tenant_username(".");
        assert_eq!(ks, None);
        assert_eq!(user, ".");

        let (ks, user) = parse_tenant_username(":");
        assert_eq!(ks, None);
        assert_eq!(user, ":");
    }

    #[test]
    fn test_parse_tenant_username_unicode() {
        let (ks, user) = parse_tenant_username("租户.用户");
        assert_eq!(ks, Some("租户".to_string()));
        assert_eq!(user, "用户");
    }

    #[test]
    fn test_parse_tenant_username_whitespace() {
        let (ks, user) = parse_tenant_username("tenant .user");
        assert_eq!(ks, Some("tenant ".to_string()));
        assert_eq!(user, "user");

        let (ks, user) = parse_tenant_username("tenant. user");
        assert_eq!(ks, Some("tenant".to_string()));
        assert_eq!(user, " user");
    }

    #[test]
    fn test_parse_tenant_username_long_names() {
        let long_tenant = "a".repeat(100);
        let long_user = "b".repeat(100);
        let input = format!("{}.{}", long_tenant, long_user);
        let (ks, user) = parse_tenant_username(&input);
        assert_eq!(ks, Some(long_tenant));
        assert_eq!(user, long_user);
    }

    #[test]
    fn test_parse_copy_command_basic() {
        let result = DynamicPgHandler::parse_copy_command("COPY users (id, name) FROM stdin");
        assert_eq!(result, Some(("users".to_string(), vec!["id".to_string(), "name".to_string()])));
    }

    #[test]
    fn test_parse_copy_command_no_columns() {
        let result = DynamicPgHandler::parse_copy_command("COPY users FROM stdin");
        assert_eq!(result, Some(("users".to_string(), vec![])));
    }

    #[test]
    fn test_parse_copy_command_with_public_schema() {
        let result = DynamicPgHandler::parse_copy_command("COPY public.users (id, name) FROM stdin");
        assert_eq!(result, Some(("users".to_string(), vec!["id".to_string(), "name".to_string()])));
    }

    #[test]
    fn test_parse_copy_command_case_insensitive() {
        let result = DynamicPgHandler::parse_copy_command("copy USERS (ID, NAME) from STDIN");
        assert_eq!(result, Some(("USERS".to_string(), vec!["ID".to_string(), "NAME".to_string()])));
    }

    #[test]
    fn test_parse_copy_command_not_copy() {
        assert_eq!(DynamicPgHandler::parse_copy_command("SELECT * FROM users"), None);
        assert_eq!(DynamicPgHandler::parse_copy_command("INSERT INTO users VALUES (1)"), None);
    }

    #[test]
    fn test_parse_copy_command_copy_to() {
        assert_eq!(DynamicPgHandler::parse_copy_command("COPY users TO stdout"), None);
    }

    #[test]
    fn test_parse_copy_command_many_columns() {
        let result = DynamicPgHandler::parse_copy_command(
            "COPY orders (id, user_id, product, quantity, price, created_at) FROM stdin"
        );
        assert_eq!(result, Some((
            "orders".to_string(),
            vec!["id".to_string(), "user_id".to_string(), "product".to_string(), 
                 "quantity".to_string(), "price".to_string(), "created_at".to_string()]
        )));
    }
}
