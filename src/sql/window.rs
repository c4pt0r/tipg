use std::collections::HashMap;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, OrderByExpr, SelectItem, Value as SqlValue, WindowType,
};

use super::expr::{compare_values, eval_expr, eval_expr_join, JoinContext};
use crate::types::{Row, TableSchema, Value};

pub(crate) struct WindowFuncInfo {
    pub proj_idx: usize,
    pub func_name: String,
    pub arg_expr: Option<Expr>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub offset_expr: Option<Expr>,
    pub default_value_expr: Option<Expr>,
}

pub(crate) fn extract_window_functions(projection: &[SelectItem]) -> Vec<WindowFuncInfo> {
    let mut result = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        let func = match item {
            SelectItem::UnnamedExpr(Expr::Function(f)) => Some(f),
            SelectItem::ExprWithAlias {
                expr: Expr::Function(f),
                ..
            } => Some(f),
            _ => None,
        };
        if let Some(f) = func {
            if let Some(WindowType::WindowSpec(spec)) = &f.over {
                let func_name = f
                    .name
                    .0
                    .last()
                    .map(|i| i.value.to_lowercase())
                    .unwrap_or_default();
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

pub(crate) fn compute_window_functions(
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
                        let val_a = eval_expr(&order_expr.expr, Some(&rows[a]), Some(schema))
                            .unwrap_or(Value::Null);
                        let val_b = eval_expr(&order_expr.expr, Some(&rows[b]), Some(schema))
                            .unwrap_or(Value::Null);
                        let cmp = compare_values(&val_a, &val_b).unwrap_or(0);
                        if cmp != 0 {
                            let asc = order_expr.asc.unwrap_or(true);
                            return if asc {
                                if cmp > 0 {
                                    std::cmp::Ordering::Greater
                                } else {
                                    std::cmp::Ordering::Less
                                }
                            } else {
                                if cmp > 0 {
                                    std::cmp::Ordering::Less
                                } else {
                                    std::cmp::Ordering::Greater
                                }
                            };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            match wf.func_name.as_str() {
                "row_number" => compute_row_number(&row_indices, wf_idx, &mut results),
                "rank" => compute_rank(rows, schema, wf, &row_indices, wf_idx, &mut results),
                "dense_rank" => {
                    compute_dense_rank(rows, schema, wf, &row_indices, wf_idx, &mut results)
                }
                "sum" => compute_sum(rows, schema, wf, &row_indices, wf_idx, &mut results)?,
                "count" => compute_count(wf, &row_indices, wf_idx, &mut results),
                "avg" => compute_avg(rows, schema, wf, &row_indices, wf_idx, &mut results)?,
                "min" => compute_min(rows, schema, wf, &row_indices, wf_idx, &mut results)?,
                "max" => compute_max(rows, schema, wf, &row_indices, wf_idx, &mut results)?,
                "lag" => compute_lag(rows, schema, wf, &row_indices, wf_idx, &mut results)?,
                "lead" => compute_lead(rows, schema, wf, &row_indices, wf_idx, &mut results)?,
                _ => return Err(anyhow!("Unsupported window function: {}", wf.func_name)),
            }
        }
    }

    Ok(results)
}

fn compute_row_number(row_indices: &[usize], wf_idx: usize, results: &mut [Vec<Value>]) {
    for (pos, &row_idx) in row_indices.iter().enumerate() {
        results[row_idx][wf_idx] = Value::Int64((pos + 1) as i64);
    }
}

fn compute_rank(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) {
    let mut current_rank = 1i64;
    let mut prev_values: Option<Vec<Value>> = None;
    for (pos, &row_idx) in row_indices.iter().enumerate() {
        let current_values: Vec<Value> = wf
            .order_by
            .iter()
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

fn compute_dense_rank(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) {
    let mut current_rank = 1i64;
    let mut prev_values: Option<Vec<Value>> = None;
    for &row_idx in row_indices {
        let current_values: Vec<Value> = wf
            .order_by
            .iter()
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

fn compute_sum(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    if wf.order_by.is_empty() {
        let mut total = 0.0f64;
        for &row_idx in row_indices {
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
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = Value::Float64(total);
        }
    } else {
        let mut running_sum = 0.0f64;
        for &row_idx in row_indices {
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
    Ok(())
}

fn compute_count(
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) {
    if wf.order_by.is_empty() {
        let total = row_indices.len() as i64;
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = Value::Int64(total);
        }
    } else {
        let mut running_count = 0i64;
        for &row_idx in row_indices {
            running_count += 1;
            results[row_idx][wf_idx] = Value::Int64(running_count);
        }
    }
}

fn compute_avg(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    if wf.order_by.is_empty() {
        let mut total_sum = 0.0f64;
        let mut total_count = 0i64;
        for &row_idx in row_indices {
            if let Some(ref arg) = wf.arg_expr {
                let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                match val {
                    Value::Int32(n) => {
                        total_sum += n as f64;
                        total_count += 1;
                    }
                    Value::Int64(n) => {
                        total_sum += n as f64;
                        total_count += 1;
                    }
                    Value::Float64(n) => {
                        total_sum += n;
                        total_count += 1;
                    }
                    Value::Null => {}
                    _ => {
                        total_count += 1;
                    }
                }
            }
        }
        let avg_val = if total_count > 0 {
            Value::Float64(total_sum / total_count as f64)
        } else {
            Value::Null
        };
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = avg_val.clone();
        }
    } else {
        let mut running_sum = 0.0f64;
        let mut running_count = 0i64;
        for &row_idx in row_indices {
            if let Some(ref arg) = wf.arg_expr {
                let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
                match val {
                    Value::Int32(n) => {
                        running_sum += n as f64;
                        running_count += 1;
                    }
                    Value::Int64(n) => {
                        running_sum += n as f64;
                        running_count += 1;
                    }
                    Value::Float64(n) => {
                        running_sum += n;
                        running_count += 1;
                    }
                    Value::Null => {}
                    _ => {
                        running_count += 1;
                    }
                }
            }
            results[row_idx][wf_idx] = if running_count > 0 {
                Value::Float64(running_sum / running_count as f64)
            } else {
                Value::Null
            };
        }
    }
    Ok(())
}

fn compute_min(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let mut min_val: Option<Value> = None;
    for &row_idx in row_indices {
        if let Some(ref arg) = wf.arg_expr {
            let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
            if !matches!(val, Value::Null) {
                min_val = Some(match &min_val {
                    None => val.clone(),
                    Some(m) => {
                        if compare_values(&val, m).unwrap_or(0) < 0 {
                            val.clone()
                        } else {
                            m.clone()
                        }
                    }
                });
            }
        }
        if !wf.order_by.is_empty() {
            results[row_idx][wf_idx] = min_val.clone().unwrap_or(Value::Null);
        }
    }
    if wf.order_by.is_empty() {
        let final_min = min_val.unwrap_or(Value::Null);
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = final_min.clone();
        }
    }
    Ok(())
}

fn compute_max(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let mut max_val: Option<Value> = None;
    for &row_idx in row_indices {
        if let Some(ref arg) = wf.arg_expr {
            let val = eval_expr(arg, Some(&rows[row_idx]), Some(schema))?;
            if !matches!(val, Value::Null) {
                max_val = Some(match &max_val {
                    None => val.clone(),
                    Some(m) => {
                        if compare_values(&val, m).unwrap_or(0) > 0 {
                            val.clone()
                        } else {
                            m.clone()
                        }
                    }
                });
            }
        }
        if !wf.order_by.is_empty() {
            results[row_idx][wf_idx] = max_val.clone().unwrap_or(Value::Null);
        }
    }
    if wf.order_by.is_empty() {
        let final_max = max_val.unwrap_or(Value::Null);
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = final_max.clone();
        }
    }
    Ok(())
}

fn compute_lag(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let offset = wf
        .offset_expr
        .as_ref()
        .and_then(|e| match e {
            Expr::Value(SqlValue::Number(n, _)) => n.parse::<usize>().ok(),
            _ => None,
        })
        .unwrap_or(1);
    let default_val = wf
        .default_value_expr
        .as_ref()
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
    Ok(())
}

fn compute_lead(
    rows: &[Row],
    schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let offset = wf
        .offset_expr
        .as_ref()
        .and_then(|e| match e {
            Expr::Value(SqlValue::Number(n, _)) => n.parse::<usize>().ok(),
            _ => None,
        })
        .unwrap_or(1);
    let default_val = wf
        .default_value_expr
        .as_ref()
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
    Ok(())
}

/// Compute window functions for JOIN context
pub(crate) fn compute_window_functions_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    window_funcs: &[WindowFuncInfo],
) -> Result<Vec<Vec<Value>>> {
    let mut results: Vec<Vec<Value>> = vec![vec![Value::Null; window_funcs.len()]; rows.len()];

    for (wf_idx, wf) in window_funcs.iter().enumerate() {
        // Build partitions
        let mut partitions: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for (row_idx, row) in rows.iter().enumerate() {
            let ctx = JoinContext {
                tables: HashMap::new(),
                column_offsets: column_offsets.clone(),
                combined_row: row,
                combined_schema,
            };
            let mut key = Vec::new();
            for expr in &wf.partition_by {
                key.push(eval_expr_join(expr, &ctx)?);
            }
            let key_bytes = bincode::serialize(&key).unwrap_or_default();
            partitions.entry(key_bytes).or_default().push(row_idx);
        }

        for (_partition_key, mut row_indices) in partitions {
            // Sort within partition if ORDER BY is specified
            if !wf.order_by.is_empty() {
                row_indices.sort_by(|&a, &b| {
                    for order_expr in &wf.order_by {
                        let ctx_a = JoinContext {
                            tables: HashMap::new(),
                            column_offsets: column_offsets.clone(),
                            combined_row: &rows[a],
                            combined_schema,
                        };
                        let ctx_b = JoinContext {
                            tables: HashMap::new(),
                            column_offsets: column_offsets.clone(),
                            combined_row: &rows[b],
                            combined_schema,
                        };
                        let val_a = eval_expr_join(&order_expr.expr, &ctx_a).unwrap_or(Value::Null);
                        let val_b = eval_expr_join(&order_expr.expr, &ctx_b).unwrap_or(Value::Null);
                        let cmp = compare_values(&val_a, &val_b).unwrap_or(0);
                        if cmp != 0 {
                            let asc = order_expr.asc.unwrap_or(true);
                            return if asc {
                                if cmp > 0 {
                                    std::cmp::Ordering::Greater
                                } else {
                                    std::cmp::Ordering::Less
                                }
                            } else {
                                if cmp > 0 {
                                    std::cmp::Ordering::Less
                                } else {
                                    std::cmp::Ordering::Greater
                                }
                            };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            // Compute the window function for this partition
            match wf.func_name.as_str() {
                "row_number" => compute_row_number(&row_indices, wf_idx, &mut results),
                "rank" => compute_rank_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                ),
                "dense_rank" => compute_dense_rank_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                ),
                "sum" => compute_sum_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                )?,
                "count" => compute_count(&wf, &row_indices, wf_idx, &mut results),
                "avg" => compute_avg_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                )?,
                "min" => compute_min_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                )?,
                "max" => compute_max_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                )?,
                "lag" => compute_lag_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                )?,
                "lead" => compute_lead_join(
                    rows,
                    column_offsets,
                    combined_schema,
                    wf,
                    &row_indices,
                    wf_idx,
                    &mut results,
                )?,
                _ => return Err(anyhow!("Unsupported window function: {}", wf.func_name)),
            }
        }
    }

    Ok(results)
}

fn compute_rank_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) {
    let mut current_rank = 1i64;
    let mut prev_values: Option<Vec<Value>> = None;
    for (pos, &row_idx) in row_indices.iter().enumerate() {
        let ctx = JoinContext {
            tables: HashMap::new(),
            column_offsets: column_offsets.clone(),
            combined_row: &rows[row_idx],
            combined_schema,
        };
        let current_values: Vec<Value> = wf
            .order_by
            .iter()
            .map(|o| eval_expr_join(&o.expr, &ctx).unwrap_or(Value::Null))
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

fn compute_dense_rank_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) {
    let mut current_rank = 1i64;
    let mut prev_values: Option<Vec<Value>> = None;
    for &row_idx in row_indices {
        let ctx = JoinContext {
            tables: HashMap::new(),
            column_offsets: column_offsets.clone(),
            combined_row: &rows[row_idx],
            combined_schema,
        };
        let current_values: Vec<Value> = wf
            .order_by
            .iter()
            .map(|o| eval_expr_join(&o.expr, &ctx).unwrap_or(Value::Null))
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

fn compute_sum_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    if wf.order_by.is_empty() {
        // Compute total for entire partition
        let mut total = 0.0f64;
        for &row_idx in row_indices {
            if let Some(ref arg) = wf.arg_expr {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: column_offsets.clone(),
                    combined_row: &rows[row_idx],
                    combined_schema,
                };
                let val = eval_expr_join(arg, &ctx)?;
                match val {
                    Value::Int32(n) => total += n as f64,
                    Value::Int64(n) => total += n as f64,
                    Value::Float64(n) => total += n,
                    _ => {}
                }
            }
        }
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = Value::Float64(total);
        }
    } else {
        // Compute running sum
        let mut running_sum = 0.0f64;
        for &row_idx in row_indices {
            if let Some(ref arg) = wf.arg_expr {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: column_offsets.clone(),
                    combined_row: &rows[row_idx],
                    combined_schema,
                };
                let val = eval_expr_join(arg, &ctx)?;
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
    Ok(())
}

fn compute_avg_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    if wf.order_by.is_empty() {
        let mut total_sum = 0.0f64;
        let mut total_count = 0i64;
        for &row_idx in row_indices {
            if let Some(ref arg) = wf.arg_expr {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: column_offsets.clone(),
                    combined_row: &rows[row_idx],
                    combined_schema,
                };
                let val = eval_expr_join(arg, &ctx)?;
                match val {
                    Value::Int32(n) => {
                        total_sum += n as f64;
                        total_count += 1;
                    }
                    Value::Int64(n) => {
                        total_sum += n as f64;
                        total_count += 1;
                    }
                    Value::Float64(n) => {
                        total_sum += n;
                        total_count += 1;
                    }
                    Value::Null => {}
                    _ => {
                        total_count += 1;
                    }
                }
            }
        }
        let avg_val = if total_count > 0 {
            Value::Float64(total_sum / total_count as f64)
        } else {
            Value::Null
        };
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = avg_val.clone();
        }
    } else {
        let mut running_sum = 0.0f64;
        let mut running_count = 0i64;
        for &row_idx in row_indices {
            if let Some(ref arg) = wf.arg_expr {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: column_offsets.clone(),
                    combined_row: &rows[row_idx],
                    combined_schema,
                };
                let val = eval_expr_join(arg, &ctx)?;
                match val {
                    Value::Int32(n) => {
                        running_sum += n as f64;
                        running_count += 1;
                    }
                    Value::Int64(n) => {
                        running_sum += n as f64;
                        running_count += 1;
                    }
                    Value::Float64(n) => {
                        running_sum += n;
                        running_count += 1;
                    }
                    Value::Null => {}
                    _ => {
                        running_count += 1;
                    }
                }
            }
            results[row_idx][wf_idx] = if running_count > 0 {
                Value::Float64(running_sum / running_count as f64)
            } else {
                Value::Null
            };
        }
    }
    Ok(())
}

fn compute_min_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let mut min_val: Option<Value> = None;
    for &row_idx in row_indices {
        if let Some(ref arg) = wf.arg_expr {
            let ctx = JoinContext {
                tables: HashMap::new(),
                column_offsets: column_offsets.clone(),
                combined_row: &rows[row_idx],
                combined_schema,
            };
            let val = eval_expr_join(arg, &ctx)?;
            if !matches!(val, Value::Null) {
                min_val = Some(match &min_val {
                    None => val.clone(),
                    Some(m) => {
                        if compare_values(&val, m).unwrap_or(0) < 0 {
                            val.clone()
                        } else {
                            m.clone()
                        }
                    }
                });
            }
        }
        if !wf.order_by.is_empty() {
            results[row_idx][wf_idx] = min_val.clone().unwrap_or(Value::Null);
        }
    }
    if wf.order_by.is_empty() {
        let final_min = min_val.unwrap_or(Value::Null);
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = final_min.clone();
        }
    }
    Ok(())
}

fn compute_max_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let mut max_val: Option<Value> = None;
    for &row_idx in row_indices {
        if let Some(ref arg) = wf.arg_expr {
            let ctx = JoinContext {
                tables: HashMap::new(),
                column_offsets: column_offsets.clone(),
                combined_row: &rows[row_idx],
                combined_schema,
            };
            let val = eval_expr_join(arg, &ctx)?;
            if !matches!(val, Value::Null) {
                max_val = Some(match &max_val {
                    None => val.clone(),
                    Some(m) => {
                        if compare_values(&val, m).unwrap_or(0) > 0 {
                            val.clone()
                        } else {
                            m.clone()
                        }
                    }
                });
            }
        }
        if !wf.order_by.is_empty() {
            results[row_idx][wf_idx] = max_val.clone().unwrap_or(Value::Null);
        }
    }
    if wf.order_by.is_empty() {
        let final_max = max_val.unwrap_or(Value::Null);
        for &row_idx in row_indices {
            results[row_idx][wf_idx] = final_max.clone();
        }
    }
    Ok(())
}

fn compute_lag_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let offset = wf
        .offset_expr
        .as_ref()
        .and_then(|e| match e {
            Expr::Value(SqlValue::Number(n, _)) => n.parse::<usize>().ok(),
            _ => None,
        })
        .unwrap_or(1);
    let default_val = wf
        .default_value_expr
        .as_ref()
        .map(|e| eval_expr(e, None, None).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);

    for (pos, &row_idx) in row_indices.iter().enumerate() {
        let val = if pos >= offset {
            let lag_row_idx = row_indices[pos - offset];
            if let Some(ref arg) = wf.arg_expr {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: column_offsets.clone(),
                    combined_row: &rows[lag_row_idx],
                    combined_schema,
                };
                eval_expr_join(arg, &ctx)?
            } else {
                Value::Null
            }
        } else {
            default_val.clone()
        };
        results[row_idx][wf_idx] = val;
    }
    Ok(())
}

fn compute_lead_join(
    rows: &[Row],
    column_offsets: &HashMap<String, usize>,
    combined_schema: &TableSchema,
    wf: &WindowFuncInfo,
    row_indices: &[usize],
    wf_idx: usize,
    results: &mut [Vec<Value>],
) -> Result<()> {
    let offset = wf
        .offset_expr
        .as_ref()
        .and_then(|e| match e {
            Expr::Value(SqlValue::Number(n, _)) => n.parse::<usize>().ok(),
            _ => None,
        })
        .unwrap_or(1);
    let default_val = wf
        .default_value_expr
        .as_ref()
        .map(|e| eval_expr(e, None, None).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);

    for (pos, &row_idx) in row_indices.iter().enumerate() {
        let val = if pos + offset < row_indices.len() {
            let lead_row_idx = row_indices[pos + offset];
            if let Some(ref arg) = wf.arg_expr {
                let ctx = JoinContext {
                    tables: HashMap::new(),
                    column_offsets: column_offsets.clone(),
                    combined_row: &rows[lead_row_idx],
                    combined_schema,
                };
                eval_expr_join(arg, &ctx)?
            } else {
                Value::Null
            }
        } else {
            default_val.clone()
        };
        results[row_idx][wf_idx] = val;
    }
    Ok(())
}
