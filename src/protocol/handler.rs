//! PostgreSQL protocol handler using pgwire

use crate::sql::{ExecuteResult, Executor, Session};
use crate::types::Value;
use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use pgwire::api::auth::{ServerParameterProvider, StartupHandler};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::data::DataRow;
use pgwire::messages::response::CommandComplete;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

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
        },
    }
}
