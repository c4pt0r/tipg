//! Expression evaluation logic

use crate::types::{Row, TableSchema, Value};
use anyhow::{anyhow, Result};
use sqlparser::ast::{BinaryOperator, Expr, JsonOperator, Value as SqlValue};
use std::collections::HashMap;

pub struct JoinContext<'a> {
    pub tables: HashMap<String, (&'a TableSchema, &'a Row)>,
    pub column_offsets: HashMap<String, usize>,
    pub combined_row: &'a Row,
    pub combined_schema: &'a TableSchema,
}

pub fn eval_expr_join(expr: &Expr, ctx: &JoinContext) -> Result<Value> {
    match expr {
        Expr::Value(v) => eval_value(v),
        Expr::Identifier(ident) => {
            if let Some(&offset) = ctx.column_offsets.get(&ident.value) {
                Ok(ctx.combined_row.values[offset].clone())
            } else {
                Err(anyhow!("Column '{}' not found or ambiguous", ident.value))
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                let table_alias = &parts[0].value;
                let col_name = &parts[1].value;
                let key = format!("{}.{}", table_alias, col_name);
                if let Some(&offset) = ctx.column_offsets.get(&key) {
                    return Ok(ctx.combined_row.values[offset].clone());
                }
                let key_lower =
                    format!("{}.{}", table_alias.to_lowercase(), col_name.to_lowercase());
                for (k, &offset) in &ctx.column_offsets {
                    if k.to_lowercase() == key_lower {
                        return Ok(ctx.combined_row.values[offset].clone());
                    }
                }
                Err(anyhow!("Column '{}.{}' not found", table_alias, col_name))
            } else {
                Err(anyhow!("Unsupported compound identifier"))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_join(left, ctx)?;
            let right_val = eval_expr_join(right, ctx)?;
            eval_binary_op(left_val, op, right_val)
        }
        Expr::UnaryOp { op, expr } => {
            let val = eval_expr_join(expr, ctx)?;
            match op {
                sqlparser::ast::UnaryOperator::Minus => match val {
                    Value::Int32(i) => Ok(Value::Int32(-i)),
                    Value::Int64(i) => Ok(Value::Int64(-i)),
                    Value::Float64(f) => Ok(Value::Float64(-f)),
                    _ => Err(anyhow!("Cannot negate {:?}", val)),
                },
                sqlparser::ast::UnaryOperator::Not => match val {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    _ => Err(anyhow!("NOT requires boolean")),
                },
                _ => Err(anyhow!("Unsupported unary operator")),
            }
        }
        Expr::Nested(expr) => eval_expr_join(expr, ctx),
        Expr::IsNull(expr) => {
            let val = eval_expr_join(expr, ctx)?;
            Ok(Value::Boolean(matches!(val, Value::Null)))
        }
        Expr::IsNotNull(expr) => {
            let val = eval_expr_join(expr, ctx)?;
            Ok(Value::Boolean(!matches!(val, Value::Null)))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_expr_join(expr, ctx)?;
            let mut found = false;
            for item in list {
                let item_val = eval_expr_join(item, ctx)?;
                if compare_values(&val, &item_val).unwrap_or(1) == 0 {
                    found = true;
                    break;
                }
            }
            Ok(Value::Boolean(if *negated { !found } else { found }))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let val = eval_expr_join(expr, ctx)?;
            let low_val = eval_expr_join(low, ctx)?;
            let high_val = eval_expr_join(high, ctx)?;
            let ge_low = compare_values(&val, &low_val).unwrap_or(-1) >= 0;
            let le_high = compare_values(&val, &high_val).unwrap_or(1) <= 0;
            let in_range = ge_low && le_high;
            Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
        }
        Expr::Function(func) => eval_function_join(func, ctx),
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let val = eval_expr_join(expr, ctx)?;
            let pat = eval_expr_join(pattern, ctx)?;
            let (Value::Text(s), Value::Text(p)) = (&val, &pat) else {
                return Ok(Value::Boolean(false));
            };
            let esc = escape_char
                .as_ref()
                .map(|c| c.to_string())
                .unwrap_or_default();
            let matched = like_match(s, p, &esc, false);
            Ok(Value::Boolean(if *negated { !matched } else { matched }))
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let val = eval_expr_join(expr, ctx)?;
            let pat = eval_expr_join(pattern, ctx)?;
            let (Value::Text(s), Value::Text(p)) = (&val, &pat) else {
                return Ok(Value::Boolean(false));
            };
            let esc = escape_char
                .as_ref()
                .map(|c| c.to_string())
                .unwrap_or_default();
            let matched = like_match(s, p, &esc, true);
            Ok(Value::Boolean(if *negated { !matched } else { matched }))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                let op_val = eval_expr_join(op, ctx)?;
                for (i, cond) in conditions.iter().enumerate() {
                    let cond_val = eval_expr_join(cond, ctx)?;
                    if compare_values(&op_val, &cond_val).unwrap_or(1) == 0 {
                        return eval_expr_join(&results[i], ctx);
                    }
                }
            } else {
                for (i, cond) in conditions.iter().enumerate() {
                    if matches!(eval_expr_join(cond, ctx)?, Value::Boolean(true)) {
                        return eval_expr_join(&results[i], ctx);
                    }
                }
            }
            if let Some(else_expr) = else_result {
                eval_expr_join(else_expr, ctx)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Cast {
            expr, data_type, ..
        } => {
            let val = eval_expr_join(expr, ctx)?;
            cast_value(val, data_type)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let val = eval_expr_join(expr, ctx)?;
            let Value::Text(s) = val else {
                return Ok(Value::Null);
            };
            let start = if let Some(from_expr) = substring_from {
                match eval_expr_join(from_expr, ctx)? {
                    Value::Int32(n) => (n - 1).max(0) as usize,
                    Value::Int64(n) => (n - 1).max(0) as usize,
                    _ => 0,
                }
            } else {
                0
            };
            let len = if let Some(for_expr) = substring_for {
                match eval_expr_join(for_expr, ctx)? {
                    Value::Int32(n) => Some(n.max(0) as usize),
                    Value::Int64(n) => Some(n.max(0) as usize),
                    _ => None,
                }
            } else {
                None
            };
            let chars: Vec<char> = s.chars().collect();
            let result: String = if let Some(l) = len {
                chars.iter().skip(start).take(l).collect()
            } else {
                chars.iter().skip(start).collect()
            };
            Ok(Value::Text(result))
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_where,
            ..
        } => {
            let val = eval_expr_join(expr, ctx)?;
            let Value::Text(s) = val else {
                return Ok(Value::Null);
            };
            let trim_chars: Vec<char> = if let Some(what) = trim_what {
                match eval_expr_join(what, ctx)? {
                    Value::Text(t) => t.chars().collect(),
                    _ => vec![' '],
                }
            } else {
                vec![' ']
            };
            let result = match trim_where {
                Some(sqlparser::ast::TrimWhereField::Leading) => s
                    .trim_start_matches(|c| trim_chars.contains(&c))
                    .to_string(),
                Some(sqlparser::ast::TrimWhereField::Trailing) => {
                    s.trim_end_matches(|c| trim_chars.contains(&c)).to_string()
                }
                Some(sqlparser::ast::TrimWhereField::Both) | None => {
                    s.trim_matches(|c| trim_chars.contains(&c)).to_string()
                }
            };
            Ok(Value::Text(result))
        }
        Expr::Position { expr, r#in } => {
            let substr = eval_expr_join(expr, ctx)?;
            let string = eval_expr_join(r#in, ctx)?;
            let (Value::Text(sub), Value::Text(s)) = (substr, string) else {
                return Ok(Value::Int32(0));
            };
            let pos = s.find(&sub).map(|i| i as i32 + 1).unwrap_or(0);
            Ok(Value::Int32(pos))
        }
        Expr::Extract { field, expr } => {
            let val = eval_expr_join(expr, ctx)?;
            let ts = match val {
                Value::Timestamp(t) => t,
                _ => return Ok(Value::Null),
            };
            use chrono::{Datelike, TimeZone, Timelike, Utc};
            let dt = Utc
                .timestamp_millis_opt(ts)
                .single()
                .ok_or_else(|| anyhow!("Invalid timestamp"))?;
            let result = match field {
                sqlparser::ast::DateTimeField::Year => dt.year() as f64,
                sqlparser::ast::DateTimeField::Month => dt.month() as f64,
                sqlparser::ast::DateTimeField::Day => dt.day() as f64,
                sqlparser::ast::DateTimeField::Hour => dt.hour() as f64,
                sqlparser::ast::DateTimeField::Minute => dt.minute() as f64,
                sqlparser::ast::DateTimeField::Second => dt.second() as f64,
                sqlparser::ast::DateTimeField::Dow => dt.weekday().num_days_from_sunday() as f64,
                sqlparser::ast::DateTimeField::Doy => dt.ordinal() as f64,
                sqlparser::ast::DateTimeField::Week => dt.iso_week().week() as f64,
                sqlparser::ast::DateTimeField::Quarter => ((dt.month() - 1) / 3 + 1) as f64,
                sqlparser::ast::DateTimeField::Epoch => ts as f64 / 1000.0,
                _ => return Err(anyhow!("Unsupported EXTRACT field")),
            };
            Ok(Value::Float64(result))
        }
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => eval_json_access_expr_join(left, operator, right, ctx),
        Expr::Array(array) => {
            let mut values = Vec::new();
            for elem in &array.elem {
                values.push(eval_expr_join(elem, ctx)?);
            }
            Ok(Value::Array(values))
        }
        Expr::ArrayIndex { obj, indexes } => {
            let arr_val = eval_expr_join(obj, ctx)?;
            eval_array_index_join(arr_val, indexes, ctx)
        }
        _ => Err(anyhow!(
            "Unsupported expression in JOIN context: {:?}",
            expr
        )),
    }
}

fn eval_function_join(func: &sqlparser::ast::Function, ctx: &JoinContext) -> Result<Value> {
    let func_name = func.name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    let args: Vec<Value> = func
        .args
        .iter()
        .filter_map(|arg| {
            if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) =
                arg
            {
                eval_expr_join(e, ctx).ok()
            } else {
                None
            }
        })
        .collect();

    match func_name.to_uppercase().as_str() {
        "COALESCE" => {
            for val in args {
                if !matches!(val, Value::Null) {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        "NULLIF" => {
            if args.len() >= 2 && compare_values(&args[0], &args[1]).unwrap_or(1) == 0 {
                Ok(Value::Null)
            } else {
                Ok(args.into_iter().next().unwrap_or(Value::Null))
            }
        }
        "GREATEST" => {
            let mut max = Value::Null;
            for val in args {
                if matches!(max, Value::Null) {
                    max = val;
                } else if compare_values(&val, &max).unwrap_or(0) > 0 {
                    max = val;
                }
            }
            Ok(max)
        }
        "LEAST" => {
            let mut min = Value::Null;
            for val in args {
                if matches!(min, Value::Null) {
                    min = val;
                } else if compare_values(&val, &min).unwrap_or(0) < 0 {
                    min = val;
                }
            }
            Ok(min)
        }
        "UPPER" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_uppercase())),
            Some(v) => Ok(v),
            None => Ok(Value::Null),
        },
        "LOWER" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_lowercase())),
            Some(v) => Ok(v),
            None => Ok(Value::Null),
        },
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Int32(s.chars().count() as i32)),
            Some(Value::Bytes(b)) => Ok(Value::Int32(b.len() as i32)),
            _ => Ok(Value::Null),
        },
        "CONCAT" => {
            let mut result = String::new();
            for val in args {
                match val {
                    Value::Null => {}
                    Value::Text(s) => result.push_str(&s),
                    v => result.push_str(&v.to_string()),
                }
            }
            Ok(Value::Text(result))
        }
        "LEFT" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let n = match iter.next() {
                Some(Value::Int32(n)) => n.max(0) as usize,
                Some(Value::Int64(n)) => n.max(0) as usize,
                _ => return Ok(Value::Null),
            };
            Ok(Value::Text(s.chars().take(n).collect()))
        }
        "RIGHT" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let n = match iter.next() {
                Some(Value::Int32(n)) => n.max(0) as usize,
                Some(Value::Int64(n)) => n.max(0) as usize,
                _ => return Ok(Value::Null),
            };
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::Text(chars[start..].iter().collect()))
        }
        "REPLACE" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let from = match iter.next() {
                Some(Value::Text(f)) => f,
                _ => return Ok(Value::Text(s)),
            };
            let to = match iter.next() {
                Some(Value::Text(t)) => t,
                _ => String::new(),
            };
            Ok(Value::Text(s.replace(&from, &to)))
        }
        "ABS" => match args.into_iter().next() {
            Some(Value::Int32(n)) => Ok(Value::Int32(n.abs())),
            Some(Value::Int64(n)) => Ok(Value::Int64(n.abs())),
            Some(Value::Float64(n)) => Ok(Value::Float64(n.abs())),
            _ => Ok(Value::Null),
        },
        "CEIL" | "CEILING" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.ceil())),
            Some(Value::Int32(n)) => Ok(Value::Int32(n)),
            Some(Value::Int64(n)) => Ok(Value::Int64(n)),
            _ => Ok(Value::Null),
        },
        "FLOOR" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.floor())),
            Some(Value::Int32(n)) => Ok(Value::Int32(n)),
            Some(Value::Int64(n)) => Ok(Value::Int64(n)),
            _ => Ok(Value::Null),
        },
        "ROUND" => {
            let mut iter = args.into_iter();
            let val = iter.next();
            let precision = match iter.next() {
                Some(Value::Int32(n)) => n,
                Some(Value::Int64(n)) => n as i32,
                _ => 0,
            };
            match val {
                Some(Value::Float64(n)) => {
                    let factor = 10_f64.powi(precision);
                    Ok(Value::Float64((n * factor).round() / factor))
                }
                Some(Value::Int32(n)) => Ok(Value::Int32(n)),
                Some(Value::Int64(n)) => Ok(Value::Int64(n)),
                _ => Ok(Value::Null),
            }
        }
        "SQRT" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.sqrt())),
            Some(Value::Int32(n)) => Ok(Value::Float64((n as f64).sqrt())),
            Some(Value::Int64(n)) => Ok(Value::Float64((n as f64).sqrt())),
            _ => Ok(Value::Null),
        },
        "NOW" | "CURRENT_TIMESTAMP" => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(Value::Timestamp(ts))
        }
        "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => {
            let uuid = uuid::Uuid::new_v4();
            Ok(Value::Uuid(*uuid.as_bytes()))
        }
        _ => Err(anyhow!("Unsupported function in JOIN: {}", func_name)),
    }
}

/// Evaluate an expression against a row
pub fn eval_expr(expr: &Expr, row: Option<&Row>, schema: Option<&TableSchema>) -> Result<Value> {
    match expr {
        Expr::Value(v) => eval_value(v),
        Expr::Identifier(ident) => {
            if let (Some(row), Some(schema)) = (row, schema) {
                let idx = schema
                    .column_index(&ident.value)
                    .ok_or_else(|| anyhow!("Column '{}' not found", ident.value))?;
                Ok(row.values[idx].clone())
            } else {
                Err(anyhow!(
                    "Cannot evaluate identifier '{}' without row context",
                    ident.value
                ))
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() >= 2 {
                let col_name = &parts[parts.len() - 1].value;
                if let (Some(row), Some(schema)) = (row, schema) {
                    let idx = schema
                        .column_index(col_name)
                        .ok_or_else(|| anyhow!("Column '{}' not found", col_name))?;
                    Ok(row.values[idx].clone())
                } else {
                    Err(anyhow!("Cannot evaluate column without row context"))
                }
            } else {
                Err(anyhow!("Invalid compound identifier"))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr(left, row, schema)?;
            let right_val = eval_expr(right, row, schema)?;
            eval_binary_op(left_val, op, right_val)
        }
        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, row, schema)?;
            match op {
                sqlparser::ast::UnaryOperator::Minus => match val {
                    Value::Int32(i) => Ok(Value::Int32(-i)),
                    Value::Int64(i) => Ok(Value::Int64(-i)),
                    Value::Float64(f) => Ok(Value::Float64(-f)),
                    _ => Err(anyhow!("Cannot negate {:?}", val)),
                },
                sqlparser::ast::UnaryOperator::Not => match val {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    _ => Err(anyhow!("NOT requires boolean, got {:?}", val)),
                },
                _ => Err(anyhow!("Unsupported unary operator: {:?}", op)),
            }
        }
        Expr::Nested(expr) => eval_expr(expr, row, schema),
        Expr::IsNull(expr) => {
            let val = eval_expr(expr, row, schema)?;
            Ok(Value::Boolean(matches!(val, Value::Null)))
        }
        Expr::IsNotNull(expr) => {
            let val = eval_expr(expr, row, schema)?;
            Ok(Value::Boolean(!matches!(val, Value::Null)))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_expr(expr, row, schema)?;
            let mut found = false;
            for item in list {
                let item_val = eval_expr(item, row, schema)?;
                if compare_values(&val, &item_val).unwrap_or(1) == 0 {
                    found = true;
                    break;
                }
            }
            Ok(Value::Boolean(if *negated { !found } else { found }))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let val = eval_expr(expr, row, schema)?;
            let low_val = eval_expr(low, row, schema)?;
            let high_val = eval_expr(high, row, schema)?;
            let ge_low = compare_values(&val, &low_val).unwrap_or(-1) >= 0;
            let le_high = compare_values(&val, &high_val).unwrap_or(1) <= 0;
            let in_range = ge_low && le_high;
            Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
        }
        Expr::Function(func) => eval_function(func, row, schema),
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let val = eval_expr(expr, row, schema)?;
            let pat = eval_expr(pattern, row, schema)?;
            let (Value::Text(s), Value::Text(p)) = (&val, &pat) else {
                return Ok(Value::Boolean(false));
            };
            let esc = escape_char
                .as_ref()
                .map(|c| c.to_string())
                .unwrap_or_default();
            let matched = like_match(s, p, &esc, false);
            Ok(Value::Boolean(if *negated { !matched } else { matched }))
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let val = eval_expr(expr, row, schema)?;
            let pat = eval_expr(pattern, row, schema)?;
            let (Value::Text(s), Value::Text(p)) = (&val, &pat) else {
                return Ok(Value::Boolean(false));
            };
            let esc = escape_char
                .as_ref()
                .map(|c| c.to_string())
                .unwrap_or_default();
            let matched = like_match(s, p, &esc, true);
            Ok(Value::Boolean(if *negated { !matched } else { matched }))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                let op_val = eval_expr(op, row, schema)?;
                for (i, cond) in conditions.iter().enumerate() {
                    let cond_val = eval_expr(cond, row, schema)?;
                    if compare_values(&op_val, &cond_val).unwrap_or(1) == 0 {
                        return eval_expr(&results[i], row, schema);
                    }
                }
            } else {
                for (i, cond) in conditions.iter().enumerate() {
                    if matches!(eval_expr(cond, row, schema)?, Value::Boolean(true)) {
                        return eval_expr(&results[i], row, schema);
                    }
                }
            }
            if let Some(else_expr) = else_result {
                eval_expr(else_expr, row, schema)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Cast {
            expr, data_type, ..
        } => {
            let val = eval_expr(expr, row, schema)?;
            cast_value(val, data_type)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let val = eval_expr(expr, row, schema)?;
            let Value::Text(s) = val else {
                return Ok(Value::Null);
            };
            let start = if let Some(from_expr) = substring_from {
                match eval_expr(from_expr, row, schema)? {
                    Value::Int32(n) => (n - 1).max(0) as usize,
                    Value::Int64(n) => (n - 1).max(0) as usize,
                    _ => 0,
                }
            } else {
                0
            };
            let len = if let Some(for_expr) = substring_for {
                match eval_expr(for_expr, row, schema)? {
                    Value::Int32(n) => Some(n.max(0) as usize),
                    Value::Int64(n) => Some(n.max(0) as usize),
                    _ => None,
                }
            } else {
                None
            };
            let chars: Vec<char> = s.chars().collect();
            let result: String = if let Some(l) = len {
                chars.iter().skip(start).take(l).collect()
            } else {
                chars.iter().skip(start).collect()
            };
            Ok(Value::Text(result))
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_where,
            ..
        } => {
            let val = eval_expr(expr, row, schema)?;
            let Value::Text(s) = val else {
                return Ok(Value::Null);
            };
            let trim_chars: Vec<char> = if let Some(what) = trim_what {
                match eval_expr(what, row, schema)? {
                    Value::Text(t) => t.chars().collect(),
                    _ => vec![' '],
                }
            } else {
                vec![' ']
            };
            let result = match trim_where {
                Some(sqlparser::ast::TrimWhereField::Leading) => s
                    .trim_start_matches(|c| trim_chars.contains(&c))
                    .to_string(),
                Some(sqlparser::ast::TrimWhereField::Trailing) => {
                    s.trim_end_matches(|c| trim_chars.contains(&c)).to_string()
                }
                Some(sqlparser::ast::TrimWhereField::Both) | None => {
                    s.trim_matches(|c| trim_chars.contains(&c)).to_string()
                }
            };
            Ok(Value::Text(result))
        }
        Expr::Position { expr, r#in } => {
            let substr = eval_expr(expr, row, schema)?;
            let string = eval_expr(r#in, row, schema)?;
            let (Value::Text(sub), Value::Text(s)) = (substr, string) else {
                return Ok(Value::Int32(0));
            };
            let pos = s.find(&sub).map(|i| i as i32 + 1).unwrap_or(0);
            Ok(Value::Int32(pos))
        }
        Expr::Extract { field, expr } => {
            let val = eval_expr(expr, row, schema)?;
            let ts = match val {
                Value::Timestamp(t) => t,
                _ => return Ok(Value::Null),
            };
            use chrono::{Datelike, TimeZone, Timelike, Utc};
            let dt = Utc
                .timestamp_millis_opt(ts)
                .single()
                .ok_or_else(|| anyhow!("Invalid timestamp"))?;
            let result = match field {
                sqlparser::ast::DateTimeField::Year => dt.year() as f64,
                sqlparser::ast::DateTimeField::Month => dt.month() as f64,
                sqlparser::ast::DateTimeField::Day => dt.day() as f64,
                sqlparser::ast::DateTimeField::Hour => dt.hour() as f64,
                sqlparser::ast::DateTimeField::Minute => dt.minute() as f64,
                sqlparser::ast::DateTimeField::Second => dt.second() as f64,
                sqlparser::ast::DateTimeField::Dow => dt.weekday().num_days_from_sunday() as f64,
                sqlparser::ast::DateTimeField::Doy => dt.ordinal() as f64,
                sqlparser::ast::DateTimeField::Week => dt.iso_week().week() as f64,
                sqlparser::ast::DateTimeField::Quarter => ((dt.month() - 1) / 3 + 1) as f64,
                sqlparser::ast::DateTimeField::Epoch => ts as f64 / 1000.0,
                _ => return Err(anyhow!("Unsupported EXTRACT field")),
            };
            Ok(Value::Float64(result))
        }
        Expr::Ceil { expr, .. } => {
            let val = eval_expr(expr, row, schema)?;
            match val {
                Value::Float64(n) => Ok(Value::Float64(n.ceil())),
                Value::Int32(n) => Ok(Value::Int32(n)),
                Value::Int64(n) => Ok(Value::Int64(n)),
                _ => Ok(Value::Null),
            }
        }
        Expr::Floor { expr, .. } => {
            let val = eval_expr(expr, row, schema)?;
            match val {
                Value::Float64(n) => Ok(Value::Float64(n.floor())),
                Value::Int32(n) => Ok(Value::Int32(n)),
                Value::Int64(n) => Ok(Value::Int64(n)),
                _ => Ok(Value::Null),
            }
        }
        Expr::Interval(interval) => {
            let val = eval_expr(&interval.value, row, schema)?;
            match val {
                Value::Text(s) => parse_interval_from_expr(&s, interval),
                Value::Int32(n) => interval_from_number(n as i64, interval),
                Value::Int64(n) => interval_from_number(n, interval),
                _ => Err(anyhow!("Invalid interval value")),
            }
        }
        Expr::TypedString { data_type, value } => match data_type {
            sqlparser::ast::DataType::Interval => parse_interval_string(value),
            sqlparser::ast::DataType::Timestamp(_, _) => parse_timestamp_string(value),
            _ => Ok(Value::Text(value.clone())),
        },
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => eval_json_access_expr(left, operator, right, row, schema),
        Expr::Array(array) => {
            let mut values = Vec::new();
            for elem in &array.elem {
                values.push(eval_expr(elem, row, schema)?);
            }
            Ok(Value::Array(values))
        }
        Expr::ArrayIndex { obj, indexes } => {
            let arr_val = eval_expr(obj, row, schema)?;
            eval_array_index(arr_val, indexes, row, schema)
        }
        _ => Err(anyhow!("Unsupported expression: {:?}", expr)),
    }
}

fn parse_interval_from_expr(s: &str, interval: &sqlparser::ast::Interval) -> Result<Value> {
    if let Some(field) = &interval.leading_field {
        let num: i64 = s.trim().parse().unwrap_or(0);
        return interval_from_field(num, field);
    }
    parse_interval_string(s)
}

fn interval_from_number(num: i64, interval: &sqlparser::ast::Interval) -> Result<Value> {
    if let Some(field) = &interval.leading_field {
        return interval_from_field(num, field);
    }
    Ok(Value::Interval(num * 1000))
}

fn interval_from_field(num: i64, field: &sqlparser::ast::DateTimeField) -> Result<Value> {
    let ms = match field {
        sqlparser::ast::DateTimeField::Year => num * 365 * 24 * 60 * 60 * 1000,
        sqlparser::ast::DateTimeField::Month => num * 30 * 24 * 60 * 60 * 1000,
        sqlparser::ast::DateTimeField::Week => num * 7 * 24 * 60 * 60 * 1000,
        sqlparser::ast::DateTimeField::Day => num * 24 * 60 * 60 * 1000,
        sqlparser::ast::DateTimeField::Hour => num * 60 * 60 * 1000,
        sqlparser::ast::DateTimeField::Minute => num * 60 * 1000,
        sqlparser::ast::DateTimeField::Second => num * 1000,
        _ => return Err(anyhow!("Unsupported interval field")),
    };
    Ok(Value::Interval(ms))
}

fn eval_function(
    func: &sqlparser::ast::Function,
    row: Option<&Row>,
    schema: Option<&TableSchema>,
) -> Result<Value> {
    let func_name = func.name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    let args: Vec<Value> = func
        .args
        .iter()
        .filter_map(|arg| {
            if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) =
                arg
            {
                eval_expr(e, row, schema).ok()
            } else {
                None
            }
        })
        .collect();

    match func_name.to_uppercase().as_str() {
        "COALESCE" => {
            for val in args {
                if !matches!(val, Value::Null) {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        "NULLIF" => {
            if args.len() >= 2 && compare_values(&args[0], &args[1]).unwrap_or(1) == 0 {
                Ok(Value::Null)
            } else {
                Ok(args.into_iter().next().unwrap_or(Value::Null))
            }
        }
        "GREATEST" => {
            let mut max = Value::Null;
            for val in args {
                if matches!(max, Value::Null) {
                    max = val;
                } else if compare_values(&val, &max).unwrap_or(0) > 0 {
                    max = val;
                }
            }
            Ok(max)
        }
        "LEAST" => {
            let mut min = Value::Null;
            for val in args {
                if matches!(min, Value::Null) {
                    min = val;
                } else if compare_values(&val, &min).unwrap_or(0) < 0 {
                    min = val;
                }
            }
            Ok(min)
        }
        "UPPER" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_uppercase())),
            Some(v) => Ok(v),
            None => Ok(Value::Null),
        },
        "LOWER" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_lowercase())),
            Some(v) => Ok(v),
            None => Ok(Value::Null),
        },
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Int32(s.chars().count() as i32)),
            Some(Value::Bytes(b)) => Ok(Value::Int32(b.len() as i32)),
            _ => Ok(Value::Null),
        },
        "OCTET_LENGTH" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Int32(s.len() as i32)),
            Some(Value::Bytes(b)) => Ok(Value::Int32(b.len() as i32)),
            _ => Ok(Value::Null),
        },
        "CONCAT" => {
            let mut result = String::new();
            for val in args {
                match val {
                    Value::Null => {}
                    Value::Text(s) => result.push_str(&s),
                    v => result.push_str(&v.to_string()),
                }
            }
            Ok(Value::Text(result))
        }
        "CONCAT_WS" => {
            let mut iter = args.into_iter();
            let sep = match iter.next() {
                Some(Value::Text(s)) => s,
                Some(Value::Null) => return Ok(Value::Null),
                _ => String::new(),
            };
            let parts: Vec<String> = iter
                .filter_map(|v| match v {
                    Value::Null => None,
                    Value::Text(s) => Some(s),
                    v => Some(v.to_string()),
                })
                .collect();
            Ok(Value::Text(parts.join(&sep)))
        }
        "LEFT" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let n = match iter.next() {
                Some(Value::Int32(n)) => n.max(0) as usize,
                Some(Value::Int64(n)) => n.max(0) as usize,
                _ => return Ok(Value::Null),
            };
            Ok(Value::Text(s.chars().take(n).collect()))
        }
        "RIGHT" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let n = match iter.next() {
                Some(Value::Int32(n)) => n.max(0) as usize,
                Some(Value::Int64(n)) => n.max(0) as usize,
                _ => return Ok(Value::Null),
            };
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::Text(chars[start..].iter().collect()))
        }
        "LPAD" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let len = match iter.next() {
                Some(Value::Int32(n)) => n.max(0) as usize,
                Some(Value::Int64(n)) => n.max(0) as usize,
                _ => return Ok(Value::Null),
            };
            let fill = match iter.next() {
                Some(Value::Text(f)) => f,
                _ => " ".to_string(),
            };
            let char_count = s.chars().count();
            if char_count >= len {
                return Ok(Value::Text(s.chars().take(len).collect()));
            }
            let pad_len = len - char_count;
            let mut result = String::new();
            let fill_chars: Vec<char> = fill.chars().collect();
            if !fill_chars.is_empty() {
                for i in 0..pad_len {
                    result.push(fill_chars[i % fill_chars.len()]);
                }
            }
            result.push_str(&s);
            Ok(Value::Text(result))
        }
        "RPAD" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let len = match iter.next() {
                Some(Value::Int32(n)) => n.max(0) as usize,
                Some(Value::Int64(n)) => n.max(0) as usize,
                _ => return Ok(Value::Null),
            };
            let fill = match iter.next() {
                Some(Value::Text(f)) => f,
                _ => " ".to_string(),
            };
            let char_count = s.chars().count();
            if char_count >= len {
                return Ok(Value::Text(s.chars().take(len).collect()));
            }
            let mut result = s.clone();
            let fill_chars: Vec<char> = fill.chars().collect();
            if !fill_chars.is_empty() {
                for i in 0..(len - char_count) {
                    result.push(fill_chars[i % fill_chars.len()]);
                }
            }
            Ok(Value::Text(result))
        }
        "REPLACE" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let from = match iter.next() {
                Some(Value::Text(f)) => f,
                _ => return Ok(Value::Text(s)),
            };
            let to = match iter.next() {
                Some(Value::Text(t)) => t,
                _ => String::new(),
            };
            Ok(Value::Text(s.replace(&from, &to)))
        }
        "REVERSE" => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.chars().rev().collect())),
            _ => Ok(Value::Null),
        },
        "REPEAT" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let n = match iter.next() {
                Some(Value::Int32(n)) => n.max(0) as usize,
                Some(Value::Int64(n)) => n.max(0) as usize,
                _ => return Ok(Value::Null),
            };
            Ok(Value::Text(s.repeat(n)))
        }
        "SPLIT_PART" => {
            let mut iter = args.into_iter();
            let s = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => return Ok(Value::Null),
            };
            let delim = match iter.next() {
                Some(Value::Text(d)) => d,
                _ => return Ok(Value::Null),
            };
            let n = match iter.next() {
                Some(Value::Int32(n)) => n,
                Some(Value::Int64(n)) => n as i32,
                _ => return Ok(Value::Null),
            };
            if n <= 0 {
                return Err(anyhow!("field position must be > 0"));
            }
            let parts: Vec<&str> = s.split(&delim).collect();
            Ok(Value::Text(
                parts
                    .get((n - 1) as usize)
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
            ))
        }
        "INITCAP" => match args.into_iter().next() {
            Some(Value::Text(s)) => {
                let mut result = String::new();
                let mut cap_next = true;
                for c in s.chars() {
                    if c.is_alphabetic() {
                        if cap_next {
                            result.push(c.to_uppercase().next().unwrap_or(c));
                        } else {
                            result.push(c.to_lowercase().next().unwrap_or(c));
                        }
                        cap_next = false;
                    } else {
                        result.push(c);
                        cap_next = true;
                    }
                }
                Ok(Value::Text(result))
            }
            _ => Ok(Value::Null),
        },
        "ABS" => match args.into_iter().next() {
            Some(Value::Int32(n)) => Ok(Value::Int32(n.abs())),
            Some(Value::Int64(n)) => Ok(Value::Int64(n.abs())),
            Some(Value::Float64(n)) => Ok(Value::Float64(n.abs())),
            _ => Ok(Value::Null),
        },
        "CEIL" | "CEILING" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.ceil())),
            Some(Value::Int32(n)) => Ok(Value::Int32(n)),
            Some(Value::Int64(n)) => Ok(Value::Int64(n)),
            _ => Ok(Value::Null),
        },
        "FLOOR" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.floor())),
            Some(Value::Int32(n)) => Ok(Value::Int32(n)),
            Some(Value::Int64(n)) => Ok(Value::Int64(n)),
            _ => Ok(Value::Null),
        },
        "ROUND" => {
            let mut iter = args.into_iter();
            let val = iter.next();
            let precision = match iter.next() {
                Some(Value::Int32(n)) => n,
                Some(Value::Int64(n)) => n as i32,
                _ => 0,
            };
            match val {
                Some(Value::Float64(n)) => {
                    let factor = 10_f64.powi(precision);
                    Ok(Value::Float64((n * factor).round() / factor))
                }
                Some(Value::Int32(n)) => Ok(Value::Int32(n)),
                Some(Value::Int64(n)) => Ok(Value::Int64(n)),
                _ => Ok(Value::Null),
            }
        }
        "TRUNC" | "TRUNCATE" => {
            let mut iter = args.into_iter();
            let val = iter.next();
            let precision = match iter.next() {
                Some(Value::Int32(n)) => n,
                Some(Value::Int64(n)) => n as i32,
                _ => 0,
            };
            match val {
                Some(Value::Float64(n)) => {
                    let factor = 10_f64.powi(precision);
                    Ok(Value::Float64((n * factor).trunc() / factor))
                }
                Some(Value::Int32(n)) => Ok(Value::Int32(n)),
                Some(Value::Int64(n)) => Ok(Value::Int64(n)),
                _ => Ok(Value::Null),
            }
        }
        "SQRT" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.sqrt())),
            Some(Value::Int32(n)) => Ok(Value::Float64((n as f64).sqrt())),
            Some(Value::Int64(n)) => Ok(Value::Float64((n as f64).sqrt())),
            _ => Ok(Value::Null),
        },
        "POWER" | "POW" => {
            let mut iter = args.into_iter();
            let base = match iter.next() {
                Some(Value::Float64(n)) => n,
                Some(Value::Int32(n)) => n as f64,
                Some(Value::Int64(n)) => n as f64,
                _ => return Ok(Value::Null),
            };
            let exp = match iter.next() {
                Some(Value::Float64(n)) => n,
                Some(Value::Int32(n)) => n as f64,
                Some(Value::Int64(n)) => n as f64,
                _ => return Ok(Value::Null),
            };
            Ok(Value::Float64(base.powf(exp)))
        }
        "EXP" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.exp())),
            Some(Value::Int32(n)) => Ok(Value::Float64((n as f64).exp())),
            Some(Value::Int64(n)) => Ok(Value::Float64((n as f64).exp())),
            _ => Ok(Value::Null),
        },
        "LN" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.ln())),
            Some(Value::Int32(n)) => Ok(Value::Float64((n as f64).ln())),
            Some(Value::Int64(n)) => Ok(Value::Float64((n as f64).ln())),
            _ => Ok(Value::Null),
        },
        "LOG" | "LOG10" => match args.into_iter().next() {
            Some(Value::Float64(n)) => Ok(Value::Float64(n.log10())),
            Some(Value::Int32(n)) => Ok(Value::Float64((n as f64).log10())),
            Some(Value::Int64(n)) => Ok(Value::Float64((n as f64).log10())),
            _ => Ok(Value::Null),
        },
        "SIGN" => match args.into_iter().next() {
            Some(Value::Int32(n)) => Ok(Value::Int32(if n > 0 {
                1
            } else if n < 0 {
                -1
            } else {
                0
            })),
            Some(Value::Int64(n)) => Ok(Value::Int64(if n > 0 {
                1
            } else if n < 0 {
                -1
            } else {
                0
            })),
            Some(Value::Float64(n)) => Ok(Value::Float64(if n > 0.0 {
                1.0
            } else if n < 0.0 {
                -1.0
            } else {
                0.0
            })),
            _ => Ok(Value::Null),
        },
        "MOD" => {
            let mut iter = args.into_iter();
            let a = iter.next();
            let b = iter.next();
            match (a, b) {
                (Some(Value::Int32(a)), Some(Value::Int32(b))) if b != 0 => Ok(Value::Int32(a % b)),
                (Some(Value::Int64(a)), Some(Value::Int64(b))) if b != 0 => Ok(Value::Int64(a % b)),
                (Some(Value::Float64(a)), Some(Value::Float64(b))) => Ok(Value::Float64(a % b)),
                _ => Ok(Value::Null),
            }
        }
        "PI" => Ok(Value::Float64(std::f64::consts::PI)),
        "RANDOM" => Ok(Value::Float64(rand::random::<f64>())),
        "NOW" | "CURRENT_TIMESTAMP" => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(Value::Timestamp(ts))
        }
        "CURRENT_DATE" => {
            use chrono::{Datelike, Utc};
            let today = Utc::now();
            let ts = chrono::NaiveDate::from_ymd_opt(today.year(), today.month(), today.day())
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis();
            Ok(Value::Timestamp(ts))
        }
        "DATE_TRUNC" => {
            let mut iter = args.into_iter();
            let field = match iter.next() {
                Some(Value::Text(s)) => s.to_lowercase(),
                _ => return Ok(Value::Null),
            };
            let ts = match iter.next() {
                Some(Value::Timestamp(t)) => t,
                _ => return Ok(Value::Null),
            };
            use chrono::{Datelike, TimeZone, Timelike, Utc};
            let dt = Utc
                .timestamp_millis_opt(ts)
                .single()
                .ok_or_else(|| anyhow!("Invalid timestamp"))?;
            let truncated = match field.as_str() {
                "year" => chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc(),
                "month" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc(),
                "day" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc(),
                "hour" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                    .unwrap()
                    .and_hms_opt(dt.hour(), 0, 0)
                    .unwrap()
                    .and_utc(),
                "minute" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                    .unwrap()
                    .and_hms_opt(dt.hour(), dt.minute(), 0)
                    .unwrap()
                    .and_utc(),
                _ => return Err(anyhow!("Unsupported DATE_TRUNC field: {}", field)),
            };
            Ok(Value::Timestamp(truncated.timestamp_millis()))
        }
        "TO_CHAR" => {
            let mut iter = args.into_iter();
            let val = iter.next();
            let _fmt = iter.next();
            match val {
                Some(Value::Timestamp(ts)) => {
                    use chrono::{TimeZone, Utc};
                    let dt = Utc
                        .timestamp_millis_opt(ts)
                        .single()
                        .ok_or_else(|| anyhow!("Invalid timestamp"))?;
                    Ok(Value::Text(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                }
                Some(v) => Ok(Value::Text(v.to_string())),
                None => Ok(Value::Null),
            }
        }
        "AGE" => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let mut iter = args.into_iter();
            let ts1 = match iter.next() {
                Some(Value::Timestamp(t)) => t,
                _ => return Ok(Value::Null),
            };
            let ts2 = match iter.next() {
                Some(Value::Timestamp(t)) => t,
                _ => SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            };
            let diff_ms = (ts2 - ts1).abs();
            let days = diff_ms / (1000 * 60 * 60 * 24);
            Ok(Value::Text(format!("{} days", days)))
        }
        "GENERATE_SERIES" => Err(anyhow!(
            "GENERATE_SERIES is a set-returning function, not supported in this context"
        )),
        "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => {
            let uuid = uuid::Uuid::new_v4();
            Ok(Value::Uuid(*uuid.as_bytes()))
        }
        "NEXTVAL" | "CURRVAL" | "SETVAL" => Ok(Value::Int64(1)),
        "SET_CONFIG" => Ok(Value::Text(String::new())),
        "PG_IS_IN_RECOVERY" => Ok(Value::Boolean(false)),
        "PG_BACKEND_PID" => Ok(Value::Int32(std::process::id() as i32)),
        "VERSION" => Ok(Value::Text("PostgreSQL 15.0 (pg-tikv)".to_string())),
        "CURRENT_DATABASE" => Ok(Value::Text("postgres".to_string())),
        "CURRENT_SCHEMA" => Ok(Value::Text("public".to_string())),
        "CURRENT_USER" | "SESSION_USER" | "USER" => Ok(Value::Text("postgres".to_string())),
        "PG_GET_USERBYID" => Ok(Value::Text("postgres".to_string())),
        "HAS_SCHEMA_PRIVILEGE" | "HAS_TABLE_PRIVILEGE" | "HAS_DATABASE_PRIVILEGE" => {
            Ok(Value::Boolean(true))
        }
        "OBJ_DESCRIPTION" | "COL_DESCRIPTION" | "SHOBJ_DESCRIPTION" => Ok(Value::Null),
        "PG_CATALOG.SET_CONFIG" => Ok(Value::Text(String::new())),
        "ARRAY_LENGTH" => {
            let mut iter = args.into_iter();
            let arr = match iter.next() {
                Some(Value::Array(a)) => a,
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Ok(Value::Null),
            };
            let dim = match iter.next() {
                Some(Value::Int32(d)) => d,
                Some(Value::Int64(d)) => d as i32,
                _ => 1,
            };
            if dim == 1 {
                Ok(Value::Int32(arr.len() as i32))
            } else {
                Ok(Value::Null)
            }
        }
        "ARRAY_UPPER" => {
            let mut iter = args.into_iter();
            let arr = match iter.next() {
                Some(Value::Array(a)) => a,
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Ok(Value::Null),
            };
            let dim = match iter.next() {
                Some(Value::Int32(d)) => d,
                Some(Value::Int64(d)) => d as i32,
                _ => 1,
            };
            if dim == 1 && !arr.is_empty() {
                Ok(Value::Int32(arr.len() as i32))
            } else {
                Ok(Value::Null)
            }
        }
        "ARRAY_LOWER" => {
            let mut iter = args.into_iter();
            let arr = match iter.next() {
                Some(Value::Array(a)) => a,
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Ok(Value::Null),
            };
            let dim = match iter.next() {
                Some(Value::Int32(d)) => d,
                Some(Value::Int64(d)) => d as i32,
                _ => 1,
            };
            if dim == 1 && !arr.is_empty() {
                Ok(Value::Int32(1))
            } else {
                Ok(Value::Null)
            }
        }
        "CARDINALITY" => match args.into_iter().next() {
            Some(Value::Array(a)) => Ok(Value::Int32(a.len() as i32)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Ok(Value::Null),
        },
        "ARRAY_POSITION" => {
            let mut iter = args.into_iter();
            let arr = match iter.next() {
                Some(Value::Array(a)) => a,
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Ok(Value::Null),
            };
            let elem = match iter.next() {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            for (i, v) in arr.iter().enumerate() {
                if compare_values(v, &elem).unwrap_or(1) == 0 {
                    return Ok(Value::Int32((i + 1) as i32));
                }
            }
            Ok(Value::Null)
        }
        "ARRAY_CAT" => {
            let mut result = Vec::new();
            for arg in args {
                match arg {
                    Value::Array(a) => result.extend(a),
                    Value::Null => {}
                    v => result.push(v),
                }
            }
            Ok(Value::Array(result))
        }
        "ARRAY_APPEND" => {
            let mut iter = args.into_iter();
            let mut arr = match iter.next() {
                Some(Value::Array(a)) => a,
                Some(Value::Null) => Vec::new(),
                _ => return Err(anyhow!("ARRAY_APPEND requires array as first argument")),
            };
            if let Some(elem) = iter.next() {
                arr.push(elem);
            }
            Ok(Value::Array(arr))
        }
        "ARRAY_PREPEND" => {
            let mut iter = args.into_iter();
            let elem = iter.next().unwrap_or(Value::Null);
            let mut arr = match iter.next() {
                Some(Value::Array(a)) => a,
                Some(Value::Null) => Vec::new(),
                _ => return Err(anyhow!("ARRAY_PREPEND requires array as second argument")),
            };
            arr.insert(0, elem);
            Ok(Value::Array(arr))
        }
        "ARRAY_REMOVE" => {
            let mut iter = args.into_iter();
            let arr = match iter.next() {
                Some(Value::Array(a)) => a,
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Ok(Value::Null),
            };
            let elem = match iter.next() {
                Some(v) => v,
                None => return Ok(Value::Array(arr)),
            };
            let result: Vec<Value> = arr
                .into_iter()
                .filter(|v| compare_values(v, &elem).unwrap_or(1) != 0)
                .collect();
            Ok(Value::Array(result))
        }
        _ => Err(anyhow!("Unsupported function: {}", func_name)),
    }
}

fn like_match(s: &str, pattern: &str, _escape: &str, case_insensitive: bool) -> bool {
    let (s, pattern) = if case_insensitive {
        (s.to_lowercase(), pattern.to_lowercase())
    } else {
        (s.to_string(), pattern.to_string())
    };
    let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
    regex::Regex::new(&format!("^{}$", regex_pattern))
        .map(|re| re.is_match(&s))
        .unwrap_or(false)
}

fn cast_value(val: Value, data_type: &sqlparser::ast::DataType) -> Result<Value> {
    use sqlparser::ast::DataType as SqlType;
    match (val, data_type) {
        (Value::Null, _) => Ok(Value::Null),
        (v, SqlType::Text | SqlType::Varchar(_) | SqlType::String(_)) => {
            Ok(Value::Text(v.to_string()))
        }
        (Value::Text(s), SqlType::Int(_) | SqlType::Integer(_)) => {
            Ok(Value::Int32(s.trim().parse().unwrap_or(0)))
        }
        (Value::Text(s), SqlType::BigInt(_) | SqlType::Int8(_)) => {
            Ok(Value::Int64(s.trim().parse().unwrap_or(0)))
        }
        (
            Value::Text(s),
            SqlType::Float(_)
            | SqlType::Double
            | SqlType::Real
            | SqlType::Numeric(_)
            | SqlType::Decimal(_),
        ) => Ok(Value::Float64(s.trim().parse().unwrap_or(0.0))),
        (Value::Text(s), SqlType::Boolean) => Ok(Value::Boolean(matches!(
            s.to_lowercase().as_str(),
            "true" | "t" | "yes" | "y" | "1"
        ))),
        (Value::Int32(n), SqlType::BigInt(_) | SqlType::Int8(_)) => Ok(Value::Int64(n as i64)),
        (Value::Int32(n), SqlType::Float(_) | SqlType::Double | SqlType::Real) => {
            Ok(Value::Float64(n as f64))
        }
        (Value::Int64(n), SqlType::Int(_) | SqlType::Integer(_)) => Ok(Value::Int32(n as i32)),
        (Value::Int64(n), SqlType::Float(_) | SqlType::Double | SqlType::Real) => {
            Ok(Value::Float64(n as f64))
        }
        (Value::Float64(n), SqlType::Int(_) | SqlType::Integer(_)) => Ok(Value::Int32(n as i32)),
        (Value::Float64(n), SqlType::BigInt(_) | SqlType::Int8(_)) => Ok(Value::Int64(n as i64)),
        (Value::Boolean(b), SqlType::Int(_) | SqlType::Integer(_)) => {
            Ok(Value::Int32(if b { 1 } else { 0 }))
        }
        (Value::Text(s), SqlType::Interval) => parse_interval_string(&s),
        (Value::Text(s), SqlType::Timestamp(_, _)) => parse_timestamp_string(&s),
        (Value::Timestamp(ts), SqlType::Timestamp(_, _)) => Ok(Value::Timestamp(ts)),
        (Value::Text(s), SqlType::Uuid) => {
            let uuid =
                uuid::Uuid::parse_str(s.trim()).map_err(|e| anyhow!("Invalid UUID: {}", e))?;
            Ok(Value::Uuid(*uuid.as_bytes()))
        }
        (Value::Uuid(bytes), SqlType::Uuid) => Ok(Value::Uuid(bytes)),
        (v, SqlType::Custom(name, _)) => {
            if let Some(ident) = name.0.last() {
                let type_name = ident.value.to_uppercase();
                match type_name.as_str() {
                    "JSON" => {
                        let s = match &v {
                            Value::Text(s) => s.clone(),
                            Value::Json(s) => s.clone(),
                            Value::Jsonb(s) => s.clone(),
                            other => other.to_string(),
                        };
                        serde_json::from_str::<serde_json::Value>(&s)
                            .map_err(|e| anyhow!("invalid input syntax for type json: {}", e))?;
                        Ok(Value::Json(s))
                    }
                    "JSONB" => {
                        let s = match &v {
                            Value::Text(s) => s.clone(),
                            Value::Json(s) => s.clone(),
                            Value::Jsonb(s) => return Ok(Value::Jsonb(s.clone())),
                            other => other.to_string(),
                        };
                        let parsed: serde_json::Value = serde_json::from_str(&s)
                            .map_err(|e| anyhow!("invalid input syntax for type jsonb: {}", e))?;
                        Ok(Value::Jsonb(parsed.to_string()))
                    }
                    _ => Ok(v),
                }
            } else {
                Ok(v)
            }
        }
        (v, SqlType::Regclass) => Ok(v),
        (v, _) => Ok(v),
    }
}

fn parse_interval_string(s: &str) -> Result<Value> {
    let s = s.trim().to_lowercase();
    let mut total_ms: i64 = 0;

    let parts: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < parts.len() {
        if let Ok(num) = parts[i].parse::<i64>() {
            if i + 1 < parts.len() {
                let unit = parts[i + 1].trim_end_matches('s');
                let ms = match unit {
                    "day" => num * 24 * 60 * 60 * 1000,
                    "hour" => num * 60 * 60 * 1000,
                    "minute" | "min" => num * 60 * 1000,
                    "second" | "sec" => num * 1000,
                    "millisecond" | "ms" => num,
                    "week" => num * 7 * 24 * 60 * 60 * 1000,
                    "month" => num * 30 * 24 * 60 * 60 * 1000,
                    "year" => num * 365 * 24 * 60 * 60 * 1000,
                    _ => return Err(anyhow!("Unknown interval unit: {}", parts[i + 1])),
                };
                total_ms += ms;
                i += 2;
            } else {
                return Err(anyhow!("Interval number without unit"));
            }
        } else {
            i += 1;
        }
    }

    Ok(Value::Interval(total_ms))
}

fn parse_timestamp_string(s: &str) -> Result<Value> {
    use chrono::{NaiveDateTime, TimeZone, Utc};
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ];
    for fmt in &formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s.trim(), fmt) {
            return Ok(Value::Timestamp(
                Utc.from_utc_datetime(&dt).timestamp_millis(),
            ));
        }
    }
    if let Ok(dt) = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d") {
        let datetime = dt.and_hms_opt(0, 0, 0).unwrap();
        return Ok(Value::Timestamp(
            Utc.from_utc_datetime(&datetime).timestamp_millis(),
        ));
    }
    Err(anyhow!("Cannot parse timestamp: {}", s))
}

pub fn eval_value_public(v: &SqlValue) -> Result<Value> {
    eval_value(v)
}

pub fn eval_binary_op_public(left: Value, op: &BinaryOperator, right: Value) -> Result<Value> {
    eval_binary_op(left, op, right)
}

fn eval_value(v: &SqlValue) -> Result<Value> {
    match v {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
        SqlValue::Number(n, _) => {
            if n.contains('.') {
                Ok(Value::Float64(n.parse()?))
            } else {
                if let Ok(i) = n.parse::<i32>() {
                    Ok(Value::Int32(i))
                } else {
                    Ok(Value::Int64(n.parse()?))
                }
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        _ => Err(anyhow!("Unsupported value literal: {:?}", v)),
    }
}

fn eval_binary_op(left: Value, op: &BinaryOperator, right: Value) -> Result<Value> {
    match op {
        // Comparison
        BinaryOperator::Eq => Ok(Value::Boolean(compare_values(&left, &right)? == 0)),
        BinaryOperator::NotEq => Ok(Value::Boolean(compare_values(&left, &right)? != 0)),
        BinaryOperator::Gt => Ok(Value::Boolean(compare_values(&left, &right)? > 0)),
        BinaryOperator::Lt => Ok(Value::Boolean(compare_values(&left, &right)? < 0)),
        BinaryOperator::GtEq => Ok(Value::Boolean(compare_values(&left, &right)? >= 0)),
        BinaryOperator::LtEq => Ok(Value::Boolean(compare_values(&left, &right)? <= 0)),

        // Logical
        BinaryOperator::And => match (left, right) {
            (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(l && r)),
            _ => Err(anyhow!("AND requires boolean operands")),
        },
        BinaryOperator::Or => match (left, right) {
            (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(l || r)),
            _ => Err(anyhow!("OR requires boolean operands")),
        },

        // Arithmetic
        BinaryOperator::Plus => add_values(left, right),
        BinaryOperator::Minus => sub_values(left, right),
        BinaryOperator::Multiply => mul_values(left, right),
        BinaryOperator::Divide => div_values(left, right),
        BinaryOperator::Modulo => mod_values(left, right),

        // String concatenation
        BinaryOperator::StringConcat => {
            let left_str = match left {
                Value::Null => return Ok(Value::Null),
                Value::Text(s) => s,
                v => v.to_string(),
            };
            let right_str = match right {
                Value::Null => return Ok(Value::Null),
                Value::Text(s) => s,
                v => v.to_string(),
            };
            Ok(Value::Text(format!("{}{}", left_str, right_str)))
        }

        _ => Err(anyhow!("Unsupported binary operator: {:?}", op)),
    }
}

// --- Arithmetic Helpers ---

fn add_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l + r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(l as i64 + r)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l + r as i64)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l + r)),
        (Value::Int32(l), Value::Float64(r)) => Ok(Value::Float64(l as f64 + r)),
        (Value::Float64(l), Value::Int32(r)) => Ok(Value::Float64(l + r as f64)),
        (Value::Timestamp(ts), Value::Interval(iv)) => Ok(Value::Timestamp(ts + iv)),
        (Value::Interval(iv), Value::Timestamp(ts)) => Ok(Value::Timestamp(ts + iv)),
        (Value::Interval(l), Value::Interval(r)) => Ok(Value::Interval(l + r)),
        _ => Err(anyhow!("Unsupported types for addition")),
    }
}

fn sub_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l - r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(l as i64 - r)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l - r as i64)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l - r)),
        (Value::Timestamp(l), Value::Timestamp(r)) => Ok(Value::Interval(l - r)),
        (Value::Timestamp(ts), Value::Interval(iv)) => Ok(Value::Timestamp(ts - iv)),
        (Value::Interval(l), Value::Interval(r)) => Ok(Value::Interval(l - r)),
        _ => Err(anyhow!("Unsupported types for subtraction")),
    }
}

fn mul_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l * r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(l as i64 * r)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l * r as i64)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l * r)),
        _ => Err(anyhow!("Unsupported types for multiplication")),
    }
}

fn div_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int32(l), Value::Int32(r)) => {
            if r == 0 {
                return Err(anyhow!("Division by zero"));
            }
            Ok(Value::Int32(l / r))
        }
        (Value::Int64(l), Value::Int64(r)) => {
            if r == 0 {
                return Err(anyhow!("Division by zero"));
            }
            Ok(Value::Int64(l / r))
        }
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l / r)),
        _ => Err(anyhow!("Unsupported types for division")),
    }
}

fn mod_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int32(l), Value::Int32(r)) => {
            if r == 0 {
                return Err(anyhow!("Modulo by zero"));
            }
            Ok(Value::Int32(l % r))
        }
        (Value::Int64(l), Value::Int64(r)) => {
            if r == 0 {
                return Err(anyhow!("Modulo by zero"));
            }
            Ok(Value::Int64(l % r))
        }
        (Value::Int32(l), Value::Int64(r)) => {
            if r == 0 {
                return Err(anyhow!("Modulo by zero"));
            }
            Ok(Value::Int64(l as i64 % r))
        }
        (Value::Int64(l), Value::Int32(r)) => {
            if r == 0 {
                return Err(anyhow!("Modulo by zero"));
            }
            Ok(Value::Int64(l % r as i64))
        }
        _ => Err(anyhow!("Unsupported types for modulo")),
    }
}

/// Compare two values. Returns:
/// - 0: equal
/// - 1: left > right
/// - -1: left < right
pub fn compare_values(left: &Value, right: &Value) -> Result<i8> {
    match (left, right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(l.cmp(r) as i8),
        (Value::Int64(l), Value::Int64(r)) => Ok(l.cmp(r) as i8),
        (Value::Int32(l), Value::Int64(r)) => Ok((*l as i64).cmp(r) as i8),
        (Value::Int64(l), Value::Int32(r)) => Ok(l.cmp(&(*r as i64)) as i8),
        (Value::Float64(l), Value::Float64(r)) => {
            Ok(l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal) as i8)
        }
        (Value::Text(l), Value::Text(r)) => Ok(l.cmp(r) as i8),
        (Value::Boolean(l), Value::Boolean(r)) => Ok(l.cmp(r) as i8),
        (Value::Timestamp(l), Value::Timestamp(r)) => Ok(l.cmp(r) as i8),
        (Value::Uuid(l), Value::Uuid(r)) => Ok(l.cmp(r) as i8),
        (Value::Null, Value::Null) => Ok(0),
        (Value::Null, _) => Ok(-1),
        (_, Value::Null) => Ok(1),
        (Value::Text(t), Value::Int32(i)) => {
            if let Ok(n) = t.parse::<i32>() {
                Ok(n.cmp(i) as i8)
            } else {
                Ok(t.cmp(&i.to_string()) as i8)
            }
        }
        (Value::Int32(i), Value::Text(t)) => {
            if let Ok(n) = t.parse::<i32>() {
                Ok(i.cmp(&n) as i8)
            } else {
                Ok(i.to_string().cmp(t) as i8)
            }
        }
        (Value::Text(t), Value::Int64(i)) => {
            if let Ok(n) = t.parse::<i64>() {
                Ok(n.cmp(i) as i8)
            } else {
                Ok(t.cmp(&i.to_string()) as i8)
            }
        }
        (Value::Int64(i), Value::Text(t)) => {
            if let Ok(n) = t.parse::<i64>() {
                Ok(i.cmp(&n) as i8)
            } else {
                Ok(i.to_string().cmp(t) as i8)
            }
        }
        (Value::Text(t), Value::Float64(f)) => {
            if let Ok(n) = t.parse::<f64>() {
                Ok(n.partial_cmp(f).unwrap_or(std::cmp::Ordering::Equal) as i8)
            } else {
                Err(anyhow!("Cannot compare"))
            }
        }
        (Value::Float64(f), Value::Text(t)) => {
            if let Ok(n) = t.parse::<f64>() {
                Ok(f.partial_cmp(&n).unwrap_or(std::cmp::Ordering::Equal) as i8)
            } else {
                Err(anyhow!("Cannot compare"))
            }
        }
        (Value::Int32(i), Value::Float64(f)) => Ok(((*i as f64)
            .partial_cmp(f)
            .unwrap_or(std::cmp::Ordering::Equal))
            as i8),
        (Value::Float64(f), Value::Int32(i)) => {
            Ok(f.partial_cmp(&(*i as f64))
                .unwrap_or(std::cmp::Ordering::Equal) as i8)
        }
        (Value::Int64(i), Value::Float64(f)) => Ok(((*i as f64)
            .partial_cmp(f)
            .unwrap_or(std::cmp::Ordering::Equal))
            as i8),
        (Value::Float64(f), Value::Int64(i)) => {
            Ok(f.partial_cmp(&(*i as f64))
                .unwrap_or(std::cmp::Ordering::Equal) as i8)
        }
        (Value::Json(_), _) | (_, Value::Json(_)) => Err(anyhow!(
            "could not identify a comparison function for type json"
        )),
        (Value::Jsonb(_), _) | (_, Value::Jsonb(_)) => Err(anyhow!(
            "could not identify an ordering operator for type jsonb"
        )),
        _ => Err(anyhow!(
            "Cannot compare distinct types: {:?} vs {:?}",
            left,
            right
        )),
    }
}

fn eval_json_access_expr(
    left: &Expr,
    operator: &JsonOperator,
    right: &Expr,
    row: Option<&Row>,
    schema: Option<&TableSchema>,
) -> Result<Value> {
    let mut ops: Vec<(&Expr, &JsonOperator)> = Vec::new();
    collect_json_ops(operator, right, &mut ops);

    let mut current = eval_expr(left, row, schema)?;
    for (key_expr, op) in ops {
        let key = eval_expr(key_expr, row, schema)?;
        current = eval_json_access(current, op, key)?;
    }
    Ok(current)
}

fn eval_json_access_expr_join(
    left: &Expr,
    operator: &JsonOperator,
    right: &Expr,
    ctx: &JoinContext,
) -> Result<Value> {
    let mut ops: Vec<(&Expr, &JsonOperator)> = Vec::new();
    collect_json_ops(operator, right, &mut ops);

    let mut current = eval_expr_join(left, ctx)?;
    for (key_expr, op) in ops {
        let key = eval_expr_join(key_expr, ctx)?;
        current = eval_json_access(current, op, key)?;
    }
    Ok(current)
}

fn collect_json_ops<'a>(
    operator: &'a JsonOperator,
    right: &'a Expr,
    ops: &mut Vec<(&'a Expr, &'a JsonOperator)>,
) {
    if let Expr::JsonAccess {
        left: inner_left,
        operator: inner_op,
        right: inner_right,
    } = right
    {
        ops.push((inner_left, operator));
        collect_json_ops(inner_op, inner_right, ops);
    } else {
        ops.push((right, operator));
    }
}

fn eval_json_access(left: Value, operator: &JsonOperator, right: Value) -> Result<Value> {
    let json_str = match &left {
        Value::Text(s) => s.clone(),
        Value::Json(s) => s.clone(),
        Value::Jsonb(s) => s.clone(),
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("JSON operators require json/jsonb operand")),
    };

    let json_val: serde_json::Value =
        serde_json::from_str(&json_str).map_err(|e| anyhow!("Invalid JSON: {}", e))?;

    match operator {
        JsonOperator::AtArrow => {
            let right_str = match &right {
                Value::Text(s) => s.clone(),
                Value::Json(s) => s.clone(),
                Value::Jsonb(s) => s.clone(),
                Value::Null => return Ok(Value::Null),
                _ => return Err(anyhow!("@> requires json/jsonb operand on right")),
            };
            let right_json: serde_json::Value = serde_json::from_str(&right_str)
                .map_err(|e| anyhow!("Invalid JSON on right side of @>: {}", e))?;
            Ok(Value::Boolean(json_contains(&json_val, &right_json)))
        }
        JsonOperator::ArrowAt => {
            let right_str = match &right {
                Value::Text(s) => s.clone(),
                Value::Json(s) => s.clone(),
                Value::Jsonb(s) => s.clone(),
                Value::Null => return Ok(Value::Null),
                _ => return Err(anyhow!("<@ requires json/jsonb operand on right")),
            };
            let right_json: serde_json::Value = serde_json::from_str(&right_str)
                .map_err(|e| anyhow!("Invalid JSON on right side of <@: {}", e))?;
            Ok(Value::Boolean(json_contains(&right_json, &json_val)))
        }
        _ => {
            let accessed = match right {
                Value::Text(key) => json_val.get(&key),
                Value::Int32(idx) => {
                    if let Some(arr) = json_val.as_array() {
                        let idx = if idx < 0 {
                            (arr.len() as i32 + idx) as usize
                        } else {
                            idx as usize
                        };
                        arr.get(idx)
                    } else {
                        None
                    }
                }
                Value::Int64(idx) => {
                    if let Some(arr) = json_val.as_array() {
                        let idx = if idx < 0 {
                            (arr.len() as i64 + idx) as usize
                        } else {
                            idx as usize
                        };
                        arr.get(idx)
                    } else {
                        None
                    }
                }
                Value::Null => return Ok(Value::Null),
                _ => return Err(anyhow!("JSON key must be text or integer")),
            };

            match accessed {
                None => Ok(Value::Null),
                Some(val) => match operator {
                    JsonOperator::Arrow => Ok(Value::Text(val.to_string())),
                    JsonOperator::LongArrow => match val {
                        serde_json::Value::Null => Ok(Value::Null),
                        serde_json::Value::Bool(b) => Ok(Value::Text(b.to_string())),
                        serde_json::Value::Number(n) => Ok(Value::Text(n.to_string())),
                        serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
                        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                            Ok(Value::Text(val.to_string()))
                        }
                    },
                    JsonOperator::HashArrow => Ok(Value::Text(val.to_string())),
                    JsonOperator::HashLongArrow => match val {
                        serde_json::Value::Null => Ok(Value::Null),
                        serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
                        other => Ok(Value::Text(other.to_string())),
                    },
                    _ => Err(anyhow!("Unsupported JSON operator: {:?}", operator)),
                },
            }
        }
    }
}

fn json_contains(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Object(obj_a), serde_json::Value::Object(obj_b)) => obj_b
            .iter()
            .all(|(k, v)| obj_a.get(k).map_or(false, |av| json_contains(av, v))),
        (serde_json::Value::Array(arr_a), serde_json::Value::Array(arr_b)) => {
            arr_b.iter().all(|bv| arr_a.iter().any(|av| av == bv))
        }
        _ => a == b,
    }
}

fn eval_array_index(
    arr_val: Value,
    indexes: &[Expr],
    row: Option<&Row>,
    schema: Option<&TableSchema>,
) -> Result<Value> {
    let Value::Array(arr) = arr_val else {
        return Err(anyhow!("Cannot index non-array value"));
    };

    let mut current = Value::Array(arr);
    for idx_expr in indexes {
        let idx_val = eval_expr(idx_expr, row, schema)?;
        let idx = match idx_val {
            Value::Int32(i) => i as i64,
            Value::Int64(i) => i,
            _ => return Err(anyhow!("Array index must be an integer")),
        };

        let Value::Array(arr) = current else {
            return Err(anyhow!("Cannot index non-array value"));
        };

        let pg_idx = (idx - 1) as usize;
        current = arr.get(pg_idx).cloned().unwrap_or(Value::Null);
    }
    Ok(current)
}

fn eval_array_index_join(arr_val: Value, indexes: &[Expr], ctx: &JoinContext) -> Result<Value> {
    let Value::Array(arr) = arr_val else {
        return Err(anyhow!("Cannot index non-array value"));
    };

    let mut current = Value::Array(arr);
    for idx_expr in indexes {
        let idx_val = eval_expr_join(idx_expr, ctx)?;
        let idx = match idx_val {
            Value::Int32(i) => i as i64,
            Value::Int64(i) => i,
            _ => return Err(anyhow!("Array index must be an integer")),
        };

        let Value::Array(arr) = current else {
            return Err(anyhow!("Cannot index non-array value"));
        };

        let pg_idx = (idx - 1) as usize;
        current = arr.get(pg_idx).cloned().unwrap_or(Value::Null);
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_expr(sql: &str) -> Expr {
        let full_sql = format!("SELECT {}", sql);
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, &full_sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &ast[0] {
            if let sqlparser::ast::SetExpr::Select(s) = &*q.body {
                if let sqlparser::ast::SelectItem::UnnamedExpr(e) = &s.projection[0] {
                    return e.clone();
                }
            }
        }
        panic!("Failed to parse expression");
    }

    #[test]
    fn test_eval_literal_values() {
        assert_eq!(
            eval_expr(&parse_expr("42"), None, None).unwrap(),
            Value::Int32(42)
        );
        assert_eq!(
            eval_expr(&parse_expr("3.14"), None, None).unwrap(),
            Value::Float64(3.14)
        );
        assert_eq!(
            eval_expr(&parse_expr("'hello'"), None, None).unwrap(),
            Value::Text("hello".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("NULL"), None, None).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_expr(&parse_expr("true"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("false"), None, None).unwrap(),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_eval_arithmetic() {
        assert_eq!(
            eval_expr(&parse_expr("1 + 2"), None, None).unwrap(),
            Value::Int32(3)
        );
        assert_eq!(
            eval_expr(&parse_expr("10 - 4"), None, None).unwrap(),
            Value::Int32(6)
        );
        assert_eq!(
            eval_expr(&parse_expr("3 * 5"), None, None).unwrap(),
            Value::Int32(15)
        );
        assert_eq!(
            eval_expr(&parse_expr("20 / 4"), None, None).unwrap(),
            Value::Int32(5)
        );
        assert_eq!(
            eval_expr(&parse_expr("17 % 5"), None, None).unwrap(),
            Value::Int32(2)
        );
    }

    #[test]
    fn test_eval_comparison() {
        assert_eq!(
            eval_expr(&parse_expr("5 > 3"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 < 3"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 = 5"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 <> 3"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 >= 5"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 <= 6"), None, None).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_eval_logical() {
        assert_eq!(
            eval_expr(&parse_expr("true AND true"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("true AND false"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("true OR false"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("false OR false"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("NOT true"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("NOT false"), None, None).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_eval_nested() {
        assert_eq!(
            eval_expr(&parse_expr("(1 + 2) * 3"), None, None).unwrap(),
            Value::Int32(9)
        );
        assert_eq!(
            eval_expr(&parse_expr("10 / (2 + 3)"), None, None).unwrap(),
            Value::Int32(2)
        );
    }

    #[test]
    fn test_eval_unary_minus() {
        assert_eq!(
            eval_expr(&parse_expr("-5"), None, None).unwrap(),
            Value::Int32(-5)
        );
        assert_eq!(
            eval_expr(&parse_expr("-3.14"), None, None).unwrap(),
            Value::Float64(-3.14)
        );
    }

    #[test]
    fn test_eval_is_null() {
        assert_eq!(
            eval_expr(&parse_expr("NULL IS NULL"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 IS NULL"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("NULL IS NOT NULL"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 IS NOT NULL"), None, None).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_eval_in_list() {
        assert_eq!(
            eval_expr(&parse_expr("5 IN (1, 3, 5, 7)"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("4 IN (1, 3, 5, 7)"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("4 NOT IN (1, 3, 5, 7)"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("'a' IN ('a', 'b', 'c')"), None, None).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_eval_between() {
        assert_eq!(
            eval_expr(&parse_expr("5 BETWEEN 1 AND 10"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("15 BETWEEN 1 AND 10"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("5 NOT BETWEEN 10 AND 20"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("1 BETWEEN 1 AND 1"), None, None).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_compare_values() {
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Int32(3)).unwrap(),
            1
        );
        assert_eq!(
            compare_values(&Value::Int32(3), &Value::Int32(5)).unwrap(),
            -1
        );
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Int32(5)).unwrap(),
            0
        );
        assert_eq!(
            compare_values(&Value::Text("b".to_string()), &Value::Text("a".to_string())).unwrap(),
            1
        );
        assert_eq!(compare_values(&Value::Null, &Value::Int32(5)).unwrap(), -1);
        assert_eq!(compare_values(&Value::Int32(5), &Value::Null).unwrap(), 1);
    }

    #[test]
    fn test_division_by_zero() {
        assert!(eval_expr(&parse_expr("5 / 0"), None, None).is_err());
        assert!(eval_expr(&parse_expr("5 % 0"), None, None).is_err());
    }

    #[test]
    fn test_mixed_type_arithmetic() {
        let result = eval_expr(&parse_expr("1 + 2.5"), None, None).unwrap();
        assert!(matches!(result, Value::Float64(f) if (f - 3.5).abs() < 0.001));
    }

    #[test]
    fn test_string_concat() {
        assert_eq!(
            eval_expr(&parse_expr("'Hello' || ' ' || 'World'"), None, None).unwrap(),
            Value::Text("Hello World".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("'Count: ' || 42"), None, None).unwrap(),
            Value::Text("Count: 42".to_string())
        );
    }

    #[test]
    fn test_case_when() {
        assert_eq!(
            eval_expr(
                &parse_expr("CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END"),
                None,
                None
            )
            .unwrap(),
            Value::Text("yes".to_string())
        );
        assert_eq!(
            eval_expr(
                &parse_expr("CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END"),
                None,
                None
            )
            .unwrap(),
            Value::Text("no".to_string())
        );
        assert_eq!(
            eval_expr(
                &parse_expr("CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"),
                None,
                None
            )
            .unwrap(),
            Value::Text("two".to_string())
        );
    }

    #[test]
    fn test_string_functions() {
        assert_eq!(
            eval_expr(&parse_expr("UPPER('hello')"), None, None).unwrap(),
            Value::Text("HELLO".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("LOWER('HELLO')"), None, None).unwrap(),
            Value::Text("hello".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("LENGTH('hello')"), None, None).unwrap(),
            Value::Int32(5)
        );
        assert_eq!(
            eval_expr(&parse_expr("CONCAT('a', 'b', 'c')"), None, None).unwrap(),
            Value::Text("abc".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("LEFT('hello', 2)"), None, None).unwrap(),
            Value::Text("he".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("RIGHT('hello', 2)"), None, None).unwrap(),
            Value::Text("lo".to_string())
        );
        assert_eq!(
            eval_expr(
                &parse_expr("REPLACE('hello world', 'world', 'there')"),
                None,
                None
            )
            .unwrap(),
            Value::Text("hello there".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("REVERSE('hello')"), None, None).unwrap(),
            Value::Text("olleh".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("REPEAT('ab', 3)"), None, None).unwrap(),
            Value::Text("ababab".to_string())
        );
    }

    #[test]
    fn test_math_functions() {
        assert_eq!(
            eval_expr(&parse_expr("ABS(-5)"), None, None).unwrap(),
            Value::Int32(5)
        );
        assert_eq!(
            eval_expr(&parse_expr("CEIL(4.3)"), None, None).unwrap(),
            Value::Float64(5.0)
        );
        assert_eq!(
            eval_expr(&parse_expr("FLOOR(4.7)"), None, None).unwrap(),
            Value::Float64(4.0)
        );
        let round_result = eval_expr(&parse_expr("ROUND(4.567, 2)"), None, None).unwrap();
        assert!(matches!(round_result, Value::Float64(f) if (f - 4.57).abs() < 0.001));
        assert_eq!(
            eval_expr(&parse_expr("SQRT(16)"), None, None).unwrap(),
            Value::Float64(4.0)
        );
        assert_eq!(
            eval_expr(&parse_expr("POWER(2, 10)"), None, None).unwrap(),
            Value::Float64(1024.0)
        );
        assert_eq!(
            eval_expr(&parse_expr("MOD(17, 5)"), None, None).unwrap(),
            Value::Int32(2)
        );
        assert_eq!(
            eval_expr(&parse_expr("SIGN(-5)"), None, None).unwrap(),
            Value::Int32(-1)
        );
    }

    #[test]
    fn test_coalesce_nullif() {
        assert_eq!(
            eval_expr(&parse_expr("COALESCE(NULL, NULL, 'default')"), None, None).unwrap(),
            Value::Text("default".to_string())
        );
        assert_eq!(
            eval_expr(
                &parse_expr("COALESCE('first', NULL, 'default')"),
                None,
                None
            )
            .unwrap(),
            Value::Text("first".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("NULLIF(5, 5)"), None, None).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_expr(&parse_expr("NULLIF(5, 3)"), None, None).unwrap(),
            Value::Int32(5)
        );
    }

    #[test]
    fn test_greatest_least() {
        assert_eq!(
            eval_expr(&parse_expr("GREATEST(1, 5, 3)"), None, None).unwrap(),
            Value::Int32(5)
        );
        assert_eq!(
            eval_expr(&parse_expr("LEAST(1, 5, 3)"), None, None).unwrap(),
            Value::Int32(1)
        );
    }

    #[test]
    fn test_like_pattern() {
        assert_eq!(
            eval_expr(&parse_expr("'hello' LIKE 'h%'"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("'hello' LIKE '%llo'"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("'hello' LIKE 'h_llo'"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("'hello' LIKE 'world'"), None, None).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_expr(&parse_expr("'hello' NOT LIKE 'world'"), None, None).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_ilike_pattern() {
        assert_eq!(
            eval_expr(&parse_expr("'Hello' ILIKE 'h%'"), None, None).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(&parse_expr("'HELLO' ILIKE '%llo'"), None, None).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_cast() {
        assert_eq!(
            eval_expr(&parse_expr("CAST(123 AS TEXT)"), None, None).unwrap(),
            Value::Text("123".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("CAST('456' AS INTEGER)"), None, None).unwrap(),
            Value::Int32(456)
        );
        assert_eq!(
            eval_expr(&parse_expr("CAST(3.14 AS INTEGER)"), None, None).unwrap(),
            Value::Int32(3)
        );
        assert_eq!(
            eval_expr(&parse_expr("'123'::int8"), None, None).unwrap(),
            Value::Int64(123)
        );
        assert_eq!(
            eval_expr(&parse_expr("'456'::bigint"), None, None).unwrap(),
            Value::Int64(456)
        );
        assert_eq!(
            eval_expr(&parse_expr("123::text"), None, None).unwrap(),
            Value::Text("123".to_string())
        );
    }

    #[test]
    fn test_trim() {
        assert_eq!(
            eval_expr(&parse_expr("TRIM('  hello  ')"), None, None).unwrap(),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn test_position() {
        assert_eq!(
            eval_expr(&parse_expr("POSITION('lo' IN 'hello')"), None, None).unwrap(),
            Value::Int32(4)
        );
        assert_eq!(
            eval_expr(&parse_expr("POSITION('xyz' IN 'hello')"), None, None).unwrap(),
            Value::Int32(0)
        );
    }

    #[test]
    fn test_substring() {
        assert_eq!(
            eval_expr(&parse_expr("SUBSTRING('hello' FROM 2 FOR 3)"), None, None).unwrap(),
            Value::Text("ell".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("SUBSTRING('hello' FROM 2)"), None, None).unwrap(),
            Value::Text("ello".to_string())
        );
    }

    #[test]
    fn test_interval_parsing() {
        let result = parse_interval_string("1 day").unwrap();
        assert_eq!(result, Value::Interval(24 * 60 * 60 * 1000));

        let result = parse_interval_string("2 hours").unwrap();
        assert_eq!(result, Value::Interval(2 * 60 * 60 * 1000));

        let result = parse_interval_string("30 minutes").unwrap();
        assert_eq!(result, Value::Interval(30 * 60 * 1000));

        let result = parse_interval_string("1 week").unwrap();
        assert_eq!(result, Value::Interval(7 * 24 * 60 * 60 * 1000));
    }

    #[test]
    fn test_interval_expression() {
        let result = eval_expr(&parse_expr("INTERVAL '1 day'"), None, None).unwrap();
        assert_eq!(result, Value::Interval(24 * 60 * 60 * 1000));

        let result = eval_expr(&parse_expr("INTERVAL '2' DAY"), None, None).unwrap();
        assert_eq!(result, Value::Interval(2 * 24 * 60 * 60 * 1000));

        let result = eval_expr(&parse_expr("INTERVAL '3' HOUR"), None, None).unwrap();
        assert_eq!(result, Value::Interval(3 * 60 * 60 * 1000));
    }

    #[test]
    fn test_timestamp_interval_arithmetic() {
        let ts = Value::Timestamp(1000 * 60 * 60 * 24);
        let iv = Value::Interval(1000 * 60 * 60);

        let result = add_values(ts.clone(), iv.clone()).unwrap();
        assert_eq!(result, Value::Timestamp(1000 * 60 * 60 * 25));

        let result = sub_values(ts.clone(), iv.clone()).unwrap();
        assert_eq!(result, Value::Timestamp(1000 * 60 * 60 * 23));
    }

    #[test]
    fn test_timestamp_cast() {
        let result = parse_timestamp_string("2024-01-01 00:00:00").unwrap();
        assert!(matches!(result, Value::Timestamp(_)));

        let result = parse_timestamp_string("2024-01-01").unwrap();
        assert!(matches!(result, Value::Timestamp(_)));
    }

    #[test]
    fn test_now_plus_interval() {
        let result = eval_expr(&parse_expr("NOW() + INTERVAL '1 DAY'"), None, None).unwrap();
        assert!(matches!(result, Value::Timestamp(_)));
    }

    #[test]
    fn test_string_concat_to_interval() {
        let result = eval_expr(&parse_expr("('1' || ' day')::interval"), None, None).unwrap();
        assert_eq!(result, Value::Interval(24 * 60 * 60 * 1000));
    }

    #[test]
    fn test_complex_datetime_expression() {
        let result = eval_expr(
            &parse_expr("now()::timestamp + ('1' || ' day')::interval"),
            None,
            None,
        )
        .unwrap();
        assert!(matches!(result, Value::Timestamp(_)));
    }

    #[test]
    fn test_int8_cast_from_int() {
        assert_eq!(
            eval_expr(&parse_expr("42::int8"), None, None).unwrap(),
            Value::Int64(42)
        );
    }

    #[test]
    fn test_int8_cast_from_text() {
        assert_eq!(
            eval_expr(&parse_expr("'999'::int8"), None, None).unwrap(),
            Value::Int64(999)
        );
    }

    #[test]
    fn test_gen_random_uuid() {
        let result = eval_expr(&parse_expr("gen_random_uuid()"), None, None).unwrap();
        assert!(matches!(result, Value::Uuid(_)));
    }

    #[test]
    fn test_uuid_cast_from_text() {
        let result = eval_expr(
            &parse_expr("'550e8400-e29b-41d4-a716-446655440000'::uuid"),
            None,
            None,
        )
        .unwrap();
        if let Value::Uuid(bytes) = result {
            let uuid = uuid::Uuid::from_bytes(bytes);
            assert_eq!(uuid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected UUID value");
        }
    }

    #[test]
    fn test_json_arrow_object_key() {
        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"name": "Alice", "age": 30}' -> 'name'"#),
                None,
                None
            )
            .unwrap(),
            Value::Text("\"Alice\"".to_string())
        );
    }

    #[test]
    fn test_json_long_arrow_object_key() {
        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"name": "Alice", "age": 30}' ->> 'name'"#),
                None,
                None
            )
            .unwrap(),
            Value::Text("Alice".to_string())
        );
    }

    #[test]
    fn test_json_arrow_array_index() {
        assert_eq!(
            eval_expr(&parse_expr(r#"'[1, 2, 3]' -> 0"#), None, None).unwrap(),
            Value::Text("1".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr(r#"'["a", "b", "c"]' -> 1"#), None, None).unwrap(),
            Value::Text("\"b\"".to_string())
        );
    }

    #[test]
    fn test_json_long_arrow_array_index() {
        assert_eq!(
            eval_expr(&parse_expr(r#"'["a", "b", "c"]' ->> 1"#), None, None).unwrap(),
            Value::Text("b".to_string())
        );
    }

    #[test]
    fn test_json_nested_access() {
        let intermediate = eval_expr(
            &parse_expr(r#"'{"user": {"name": "Bob"}}' -> 'user'"#),
            None,
            None,
        )
        .unwrap();
        assert_eq!(intermediate, Value::Text("{\"name\":\"Bob\"}".to_string()));

        assert_eq!(
            eval_expr(&parse_expr(r#"'{"name": "Bob"}' ->> 'name'"#), None, None).unwrap(),
            Value::Text("Bob".to_string())
        );

        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"user": {"name": "Bob"}}' -> 'user' ->> 'name'"#),
                None,
                None
            )
            .unwrap(),
            Value::Text("Bob".to_string())
        );
    }

    #[test]
    fn test_json_null_key() {
        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"name": "Alice"}' -> 'missing'"#),
                None,
                None
            )
            .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_json_number_extraction() {
        assert_eq!(
            eval_expr(&parse_expr(r#"'{"count": 42}' ->> 'count'"#), None, None).unwrap(),
            Value::Text("42".to_string())
        );
    }

    #[test]
    fn test_array_literal() {
        assert_eq!(
            eval_expr(&parse_expr("ARRAY[1, 2, 3]"), None, None).unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
        );
        assert_eq!(
            eval_expr(&parse_expr("ARRAY['a', 'b', 'c']"), None, None).unwrap(),
            Value::Array(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
                Value::Text("c".to_string())
            ])
        );
    }

    #[test]
    fn test_array_indexing() {
        assert_eq!(
            eval_expr(&parse_expr("(ARRAY[10, 20, 30])[2]"), None, None).unwrap(),
            Value::Int32(20)
        );
        assert_eq!(
            eval_expr(&parse_expr("(ARRAY['a', 'b', 'c'])[1]"), None, None).unwrap(),
            Value::Text("a".to_string())
        );
        assert_eq!(
            eval_expr(&parse_expr("(ARRAY[1, 2, 3])[5]"), None, None).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_array_length() {
        assert_eq!(
            eval_expr(&parse_expr("array_length(ARRAY[1, 2, 3], 1)"), None, None).unwrap(),
            Value::Int32(3)
        );
    }

    #[test]
    fn test_array_position() {
        assert_eq!(
            eval_expr(
                &parse_expr("array_position(ARRAY['a', 'b', 'c'], 'b')"),
                None,
                None
            )
            .unwrap(),
            Value::Int32(2)
        );
        assert_eq!(
            eval_expr(&parse_expr("array_position(ARRAY[1, 2, 3], 5)"), None, None).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_array_cat() {
        assert_eq!(
            eval_expr(
                &parse_expr("array_cat(ARRAY[1, 2], ARRAY[3, 4])"),
                None,
                None
            )
            .unwrap(),
            Value::Array(vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4)
            ])
        );
    }

    #[test]
    fn test_array_append_prepend() {
        assert_eq!(
            eval_expr(&parse_expr("array_append(ARRAY[1, 2], 3)"), None, None).unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
        );
        assert_eq!(
            eval_expr(&parse_expr("array_prepend(0, ARRAY[1, 2])"), None, None).unwrap(),
            Value::Array(vec![Value::Int32(0), Value::Int32(1), Value::Int32(2)])
        );
    }

    #[test]
    fn test_cardinality() {
        assert_eq!(
            eval_expr(&parse_expr("cardinality(ARRAY[1, 2, 3, 4])"), None, None).unwrap(),
            Value::Int32(4)
        );
    }

    #[test]
    fn test_json_cast() {
        assert_eq!(
            eval_expr(&parse_expr(r#"'{"a": 1}'::json ->> 'a'"#), None, None).unwrap(),
            Value::Text("1".to_string())
        );
    }

    #[test]
    fn test_jsonb_cast() {
        assert_eq!(
            eval_expr(&parse_expr(r#"'{"b": 2}'::jsonb ->> 'b'"#), None, None).unwrap(),
            Value::Text("2".to_string())
        );
    }

    #[test]
    fn test_json_comparison_blocked() {
        let json_val = Value::Json(r#"{"a":1}"#.to_string());
        let int_val = Value::Int32(1);
        assert!(compare_values(&json_val, &int_val).is_err());
    }

    #[test]
    fn test_jsonb_comparison_blocked() {
        let jsonb_val = Value::Jsonb(r#"{"a":1}"#.to_string());
        let int_val = Value::Int32(1);
        assert!(compare_values(&jsonb_val, &int_val).is_err());
    }

    #[test]
    fn test_json_contains_at_arrow() {
        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"a":1,"b":2}'::jsonb @> '{"a":1}'::jsonb"#),
                None,
                None
            )
            .unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"a":1}'::jsonb @> '{"a":1,"b":2}'::jsonb"#),
                None,
                None
            )
            .unwrap(),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_json_contained_by_arrow_at() {
        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb"#),
                None,
                None
            )
            .unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_expr(
                &parse_expr(r#"'{"a":1,"b":2}'::jsonb <@ '{"a":1}'::jsonb"#),
                None,
                None
            )
            .unwrap(),
            Value::Boolean(false)
        );
    }
}
