//! Helper functions for SQL execution
//!
//! This module contains utility functions extracted from executor.rs
//! to reduce code size and improve maintainability.

use std::collections::HashSet;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    BinaryOperator, DataType as SqlDataType, Expr, FunctionArg, FunctionArgExpr, Value as SqlValue,
};

use super::expr::{eval_expr, eval_expr_join, JoinContext};
use super::Aggregator;
use crate::types::{ColumnDef, DataType, Row, TableSchema, Value};

/// Deduplicate rows based on their serialized values
pub fn dedup_rows(rows: Vec<Row>) -> Vec<Row> {
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

/// Coerce a value to match the expected column type
pub fn coerce_value_for_column(val: Value, col: &ColumnDef) -> Result<Value> {
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

/// Convert a Value to a SQL expression for re-parsing
pub fn value_to_sql_expr(v: &Value) -> Expr {
    match v {
        Value::Null => Expr::Value(SqlValue::Null),
        Value::Boolean(b) => Expr::Value(SqlValue::Boolean(*b)),
        Value::Int32(i) => Expr::Value(SqlValue::Number(i.to_string(), false)),
        Value::Int64(i) => Expr::Value(SqlValue::Number(i.to_string(), false)),
        Value::Float64(f) => Expr::Value(SqlValue::Number(f.to_string(), false)),
        Value::Text(s) => Expr::Value(SqlValue::SingleQuotedString(s.clone())),
        Value::Bytes(b) => Expr::Value(SqlValue::SingleQuotedString(format!(
            "\\x{}",
            hex::encode(b)
        ))),
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

/// Parse a PostgreSQL array literal string into a Vec<Value>
pub fn parse_pg_array(s: &str) -> Result<Vec<Value>> {
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

/// Parse a single array element string into a Value
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

/// Convert a SQL data type to our internal DataType
pub fn convert_data_type(sql_type: &SqlDataType) -> Result<DataType> {
    match sql_type {
        SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::SmallInt(_) | SqlDataType::Int(_) | SqlDataType::Integer(_) => {
            Ok(DataType::Int32)
        }
        SqlDataType::BigInt(_) => Ok(DataType::Int64),
        SqlDataType::Float(_)
        | SqlDataType::Double
        | SqlDataType::Real
        | SqlDataType::Numeric(_)
        | SqlDataType::Decimal(_) => Ok(DataType::Float64),
        SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharacterVarying(_) => Ok(DataType::Text),
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
                    _ => Ok(DataType::Text),
                }
            } else {
                Ok(DataType::Text)
            }
        }
        SqlDataType::Array(inner) => match inner {
            sqlparser::ast::ArrayElemTypeDef::AngleBracket(inner_type) => {
                convert_data_type(inner_type)
            }
            sqlparser::ast::ArrayElemTypeDef::SquareBracket(inner_type) => {
                convert_data_type(inner_type)
            }
            _ => Ok(DataType::Text),
        },
        _ => Err(anyhow!("Unsupported data type: {:?}", sql_type)),
    }
}

/// Extract equality conditions from a WHERE clause for index lookup
pub fn extract_eq_conditions(expr: &Expr, index_cols: &[String]) -> Option<Vec<Value>> {
    let mut values = vec![None; index_cols.len()];
    extract_conditions_recursive(expr, index_cols, &mut values);

    if values.iter().all(|v| v.is_some()) {
        Some(values.into_iter().map(|v| v.unwrap()).collect())
    } else {
        None
    }
}

fn extract_conditions_recursive(
    expr: &Expr,
    index_cols: &[String],
    values: &mut Vec<Option<Value>>,
) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
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
        },
        Expr::Nested(e) => extract_conditions_recursive(e, index_cols, values),
        _ => {}
    }
}

/// Collect aggregate functions from HAVING clause that aren't already in projection
pub fn collect_having_agg_funcs(
    expr: &Expr,
    agg_funcs: &mut Vec<(usize, sqlparser::ast::Function)>,
    extra_start: usize,
) {
    tracing::debug!("collect_having_agg_funcs called with expr: {:?}", expr);
    match expr {
        Expr::Function(f) if f.over.is_none() => {
            let func_name = f
                .name
                .0
                .last()
                .map(|i| i.value.to_uppercase())
                .unwrap_or_default();
            tracing::debug!("Found function in HAVING: {}", func_name);
            if matches!(func_name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                let already_exists = agg_funcs.iter().any(|(_, existing)| {
                    let existing_name = existing
                        .name
                        .0
                        .last()
                        .map(|n| n.value.to_uppercase())
                        .unwrap_or_default();
                    existing_name == func_name && args_match(f, existing)
                });
                tracing::debug!(
                    "Already exists: {}, agg_funcs len: {}",
                    already_exists,
                    agg_funcs.len()
                );
                if !already_exists {
                    let new_idx = extra_start
                        + (agg_funcs.len()
                            - agg_funcs
                                .iter()
                                .filter(|(idx, _)| *idx < extra_start)
                                .count());
                    tracing::debug!("Adding new agg func at index {}", new_idx);
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
            tracing::debug!("Other expr type in HAVING: {:?}", expr);
        }
    }
}

/// Evaluate HAVING clause expression with aggregated values
pub fn eval_having_expr(
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
            let func_name = f
                .name
                .0
                .last()
                .map(|i| i.value.to_uppercase())
                .unwrap_or_default();
            for (i, (_, agg_f)) in agg_funcs.iter().enumerate() {
                let agg_name = agg_f
                    .name
                    .0
                    .last()
                    .map(|n| n.value.to_uppercase())
                    .unwrap_or_default();
                if agg_name == func_name && args_match(f, agg_f) {
                    return Ok(aggs[i].result());
                }
            }
            let mut temp_agg = Aggregator::new(&func_name)?;
            let arg_expr = if f.args.is_empty() {
                None
            } else {
                match &f.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
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

/// Evaluate HAVING clause expression for join queries
pub fn eval_having_expr_join(
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
            let func_name = f
                .name
                .0
                .last()
                .map(|i| i.value.to_uppercase())
                .unwrap_or_default();
            for (i, (_, agg_f)) in agg_funcs.iter().enumerate() {
                let agg_name = agg_f
                    .name
                    .0
                    .last()
                    .map(|n| n.value.to_uppercase())
                    .unwrap_or_default();
                if agg_name == func_name && args_match(f, agg_f) {
                    return Ok(aggs[i].result());
                }
            }
            let mut temp_agg = Aggregator::new(&func_name)?;
            let arg_expr = if f.args.is_empty() {
                None
            } else {
                match &f.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
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

/// Check if two function arguments match
pub fn args_match(f1: &sqlparser::ast::Function, f2: &sqlparser::ast::Function) -> bool {
    if f1.args.len() != f2.args.len() {
        return false;
    }
    for (a1, a2) in f1.args.iter().zip(f2.args.iter()) {
        match (a1, a2) {
            (
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard),
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard),
            ) => {}
            (
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e1)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e2)),
            ) => {
                if format!("{:?}", e1) != format!("{:?}", e2) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

/// Infer the DataType from a Value
pub fn infer_data_type(value: &Value) -> DataType {
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

/// Check if a SQL statement should be skipped
pub fn get_skip_reason(sql_upper: &str) -> Option<String> {
    if sql_upper.starts_with("DROP DATABASE") {
        return Some("DROP DATABASE not supported".into());
    }
    if sql_upper.starts_with("CREATE DATABASE") {
        return Some("CREATE DATABASE not supported".into());
    }
    if sql_upper.starts_with("ALTER DATABASE") {
        return Some("ALTER DATABASE not supported".into());
    }
    if sql_upper.starts_with("\\") {
        return Some("psql meta-command not supported".into());
    }
    if sql_upper.starts_with("COPY ") || sql_upper.contains(" FROM STDIN") {
        return Some("COPY not supported".into());
    }
    None
}

/// Check if a SQL statement is unsupported
pub fn get_unsupported_reason(sql_upper: &str) -> Option<String> {
    if sql_upper.starts_with("CREATE TRIGGER") {
        return Some("CREATE TRIGGER not supported".into());
    }
    if sql_upper.starts_with("CREATE DOMAIN") {
        return Some("CREATE DOMAIN not supported".into());
    }
    if sql_upper.starts_with("CREATE AGGREGATE") {
        return Some("CREATE AGGREGATE not supported".into());
    }
    if sql_upper.starts_with("ALTER TYPE") {
        return Some("ALTER TYPE not supported".into());
    }
    if sql_upper.starts_with("ALTER DOMAIN") {
        return Some("ALTER DOMAIN not supported".into());
    }
    if sql_upper.starts_with("ALTER AGGREGATE") {
        return Some("ALTER AGGREGATE not supported".into());
    }
    if sql_upper.starts_with("ALTER FUNCTION") {
        return Some("ALTER FUNCTION not supported".into());
    }
    if sql_upper.starts_with("ALTER SEQUENCE") {
        return Some("ALTER SEQUENCE not supported".into());
    }
    if sql_upper.starts_with("ALTER TABLE") && sql_upper.contains("OWNER TO") {
        return Some("ALTER TABLE OWNER TO not supported".into());
    }
    if sql_upper.starts_with("CREATE TYPE") && sql_upper.contains("AS ENUM") {
        return Some("CREATE TYPE AS ENUM not supported".into());
    }
    if sql_upper.starts_with("CREATE TYPE") && sql_upper.contains("AS (") {
        return Some("CREATE TYPE AS composite not supported".into());
    }
    if sql_upper.contains("$_$") || sql_upper.contains("$$") {
        return Some("Dollar-quoted strings not supported".into());
    }
    if sql_upper.starts_with("CREATE SEQUENCE") && sql_upper.contains("INCREMENT") {
        return Some("CREATE SEQUENCE not supported".into());
    }
    if sql_upper.starts_with("CREATE INDEX") && sql_upper.contains("USING GIST") {
        return Some("GIST index not supported".into());
    }
    None
}

/// Check if a SQL data type is a SERIAL type
pub fn is_serial_type(sql_type: &SqlDataType) -> bool {
    match sql_type {
        SqlDataType::Custom(name, _) => {
            if let Some(ident) = name.0.last() {
                ident.value.eq_ignore_ascii_case("SERIAL")
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Get the name of a SelectItem for column naming
pub fn get_select_item_name(item: &sqlparser::ast::SelectItem) -> String {
    match item {
        sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        sqlparser::ast::SelectItem::UnnamedExpr(expr) => get_expr_name(expr),
        sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
        _ => "?column?".to_string(),
    }
}

/// Get the name of an expression for column naming
pub fn get_expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::Function(f) => f.name.to_string().to_lowercase(),
        _ => "?column?".to_string(),
    }
}

/// Fill default values for missing columns in a row
pub fn fill_row_defaults(row: &mut Row, schema: &TableSchema) -> Result<()> {
    if row.values.len() < schema.columns.len() {
        for i in row.values.len()..schema.columns.len() {
            let col = &schema.columns[i];
            let val = if let Some(expr_str) = &col.default_expr {
                eval_default_expr(expr_str)?
            } else {
                Value::Null
            };
            row.values.push(val);
        }
    }
    Ok(())
}

/// Evaluate a default expression string
pub fn eval_default_expr(expr_str: &str) -> Result<Value> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let sql = format!("SELECT {}", expr_str);
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| anyhow!("Failed to parse default expr: {}", e))?;

    if let Some(sqlparser::ast::Statement::Query(q)) = ast.into_iter().next() {
        if let sqlparser::ast::SetExpr::Select(s) = *q.body {
            if let Some(sqlparser::ast::SelectItem::UnnamedExpr(e)) =
                s.projection.into_iter().next()
            {
                return eval_expr(&e, None, None);
            }
        }
    }
    Ok(Value::Text(expr_str.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dedup_rows() {
        let rows = vec![
            Row {
                values: vec![Value::Int32(1), Value::Text("a".to_string())],
            },
            Row {
                values: vec![Value::Int32(1), Value::Text("a".to_string())],
            },
            Row {
                values: vec![Value::Int32(2), Value::Text("b".to_string())],
            },
        ];
        let result = dedup_rows(rows);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_parse_pg_array() {
        let result = parse_pg_array("{1,2,3}").unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Value::Int32(1));
        assert_eq!(result[1], Value::Int32(2));
        assert_eq!(result[2], Value::Int32(3));
    }

    #[test]
    fn test_parse_pg_array_empty() {
        let result = parse_pg_array("{}").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_pg_array_strings() {
        let result = parse_pg_array("{hello,world}").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Value::Text("hello".to_string()));
        assert_eq!(result[1], Value::Text("world".to_string()));
    }

    #[test]
    fn test_infer_data_type() {
        assert_eq!(infer_data_type(&Value::Int32(1)), DataType::Int32);
        assert_eq!(infer_data_type(&Value::Int64(1)), DataType::Int64);
        assert_eq!(infer_data_type(&Value::Float64(1.0)), DataType::Float64);
        assert_eq!(infer_data_type(&Value::Boolean(true)), DataType::Boolean);
        assert_eq!(
            infer_data_type(&Value::Text("".to_string())),
            DataType::Text
        );
    }

    #[test]
    fn test_get_skip_reason() {
        assert!(get_skip_reason("DROP DATABASE test").is_some());
        assert!(get_skip_reason("CREATE DATABASE test").is_some());
        assert!(get_skip_reason("SELECT * FROM foo").is_none());
    }

    #[test]
    fn test_get_unsupported_reason() {
        assert!(get_unsupported_reason("CREATE TRIGGER foo").is_some());
        assert!(get_unsupported_reason("CREATE DOMAIN foo").is_some());
        assert!(get_unsupported_reason("SELECT * FROM foo").is_none());
    }
}

pub fn parse_value_for_copy(val: &str, data_type: &DataType) -> Value {
    let unescaped = val
        .replace("\\t", "\t")
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\\\", "\\");

    match data_type {
        DataType::Boolean => match unescaped.to_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Value::Boolean(true),
            "f" | "false" | "0" | "no" | "off" => Value::Boolean(false),
            _ => Value::Text(unescaped),
        },
        DataType::Int32 => unescaped
            .parse::<i32>()
            .map(Value::Int32)
            .unwrap_or(Value::Text(unescaped)),
        DataType::Int64 => unescaped
            .parse::<i64>()
            .map(Value::Int64)
            .unwrap_or(Value::Text(unescaped)),
        DataType::Float64 => unescaped
            .parse::<f64>()
            .map(Value::Float64)
            .unwrap_or(Value::Text(unescaped)),
        DataType::Timestamp => {
            if let Ok(ts) =
                chrono::NaiveDateTime::parse_from_str(&unescaped, "%Y-%m-%d %H:%M:%S%.f")
            {
                Value::Timestamp(ts.and_utc().timestamp_millis())
            } else if let Ok(ts) =
                chrono::NaiveDateTime::parse_from_str(&unescaped, "%Y-%m-%d %H:%M:%S")
            {
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
                hex::decode(&unescaped[2..])
                    .map(Value::Bytes)
                    .unwrap_or(Value::Bytes(unescaped.into_bytes()))
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
