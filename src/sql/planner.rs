//! Query planner for cost-based optimization
//!
//! This module provides query planning and optimization capabilities:
//! - Cost-based index selection
//! - Predicate analysis
//! - Access path selection

use std::collections::HashMap;

use sqlparser::ast::{BinaryOperator, Expr};

use super::expr::eval_expr;
use crate::types::{IndexDef, TableSchema, Value};

#[derive(Debug, Clone)]
pub enum ScanType {
    FullTableScan,
    IndexScan {
        index_id: u64,
        index_name: String,
        values: Vec<Value>,
        estimated_rows: usize,
    },
    IndexRangeScan {
        index_id: u64,
        index_name: String,
        prefix_values: Vec<Value>,
        estimated_rows: usize,
    },
}

#[derive(Debug, Clone)]
pub struct AccessPath {
    pub scan_type: ScanType,
    pub cost: f64,
}

#[derive(Debug, Clone)]
pub struct PredicateInfo {
    pub column: String,
    pub op: PredicateOp,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredicateOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
    IsNull,
    IsNotNull,
}

pub fn analyze_predicates(expr: &Expr) -> Vec<PredicateInfo> {
    let mut predicates = Vec::new();
    collect_predicates(expr, &mut predicates);
    predicates
}

fn collect_predicates(expr: &Expr, predicates: &mut Vec<PredicateInfo>) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                collect_predicates(left, predicates);
                collect_predicates(right, predicates);
            }
            BinaryOperator::Or => {}
            BinaryOperator::Eq => {
                if let Some(pred) = extract_simple_predicate(left, right, PredicateOp::Eq) {
                    predicates.push(pred);
                }
            }
            BinaryOperator::NotEq => {
                if let Some(pred) = extract_simple_predicate(left, right, PredicateOp::Ne) {
                    predicates.push(pred);
                }
            }
            BinaryOperator::Lt => {
                if let Some(pred) = extract_simple_predicate(left, right, PredicateOp::Lt) {
                    predicates.push(pred);
                }
            }
            BinaryOperator::LtEq => {
                if let Some(pred) = extract_simple_predicate(left, right, PredicateOp::Le) {
                    predicates.push(pred);
                }
            }
            BinaryOperator::Gt => {
                if let Some(pred) = extract_simple_predicate(left, right, PredicateOp::Gt) {
                    predicates.push(pred);
                }
            }
            BinaryOperator::GtEq => {
                if let Some(pred) = extract_simple_predicate(left, right, PredicateOp::Ge) {
                    predicates.push(pred);
                }
            }
            _ => {}
        },
        Expr::IsNull(inner) => {
            if let Expr::Identifier(ident) = &**inner {
                predicates.push(PredicateInfo {
                    column: ident.value.clone(),
                    op: PredicateOp::IsNull,
                    value: Value::Null,
                });
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Identifier(ident) = &**inner {
                predicates.push(PredicateInfo {
                    column: ident.value.clone(),
                    op: PredicateOp::IsNotNull,
                    value: Value::Null,
                });
            }
        }
        Expr::Nested(e) => collect_predicates(e, predicates),
        _ => {}
    }
}

fn extract_simple_predicate(left: &Expr, right: &Expr, op: PredicateOp) -> Option<PredicateInfo> {
    if let Expr::Identifier(ident) = left {
        if let Ok(val) = eval_expr(right, None, None) {
            return Some(PredicateInfo {
                column: ident.value.clone(),
                op,
                value: val,
            });
        }
    }
    if let Expr::Identifier(ident) = right {
        if let Ok(val) = eval_expr(left, None, None) {
            let reversed_op = match op {
                PredicateOp::Lt => PredicateOp::Gt,
                PredicateOp::Le => PredicateOp::Ge,
                PredicateOp::Gt => PredicateOp::Lt,
                PredicateOp::Ge => PredicateOp::Le,
                other => other,
            };
            return Some(PredicateInfo {
                column: ident.value.clone(),
                op: reversed_op,
                value: val,
            });
        }
    }
    None
}

pub fn choose_best_access_path(
    schema: &TableSchema,
    predicates: &[PredicateInfo],
    estimated_table_rows: usize,
) -> AccessPath {
    let mut best_path = AccessPath {
        scan_type: ScanType::FullTableScan,
        cost: estimated_table_rows as f64,
    };

    let predicate_map: HashMap<&str, &PredicateInfo> = predicates
        .iter()
        .filter(|p| p.op == PredicateOp::Eq)
        .map(|p| (p.column.as_str(), p))
        .collect();

    for index in &schema.indexes {
        if let Some((scan_type, cost)) = evaluate_index(index, &predicate_map, estimated_table_rows)
        {
            if cost < best_path.cost {
                best_path = AccessPath { scan_type, cost };
            }
        }
    }

    best_path
}

fn evaluate_index(
    index: &IndexDef,
    predicate_map: &HashMap<&str, &PredicateInfo>,
    estimated_table_rows: usize,
) -> Option<(ScanType, f64)> {
    let mut matched_values = Vec::new();
    let mut all_matched = true;

    for col in &index.columns {
        if let Some(pred) = predicate_map.get(col.as_str()) {
            matched_values.push(pred.value.clone());
        } else {
            all_matched = false;
            break;
        }
    }

    if matched_values.is_empty() {
        return None;
    }

    let selectivity = estimate_selectivity(index, matched_values.len(), all_matched);
    let estimated_rows = ((estimated_table_rows as f64) * selectivity).max(1.0) as usize;

    let index_lookup_cost = 1.0;
    let row_fetch_cost = estimated_rows as f64 * 0.5;
    let cost = index_lookup_cost + row_fetch_cost;

    if all_matched {
        Some((
            ScanType::IndexScan {
                index_id: index.id,
                index_name: index.name.clone(),
                values: matched_values,
                estimated_rows,
            },
            cost,
        ))
    } else {
        Some((
            ScanType::IndexRangeScan {
                index_id: index.id,
                index_name: index.name.clone(),
                prefix_values: matched_values,
                estimated_rows,
            },
            cost,
        ))
    }
}

fn estimate_selectivity(index: &IndexDef, matched_cols: usize, full_match: bool) -> f64 {
    let base_selectivity = if index.unique && full_match {
        1.0 / 1000000.0
    } else {
        0.1_f64.powi(matched_cols as i32)
    };

    base_selectivity.max(0.0001)
}

pub fn extract_index_values(
    predicates: &[PredicateInfo],
    index_columns: &[String],
) -> Option<Vec<Value>> {
    let mut values = Vec::with_capacity(index_columns.len());

    for col in index_columns {
        let pred = predicates
            .iter()
            .find(|p| p.column == *col && p.op == PredicateOp::Eq)?;
        values.push(pred.value.clone());
    }

    Some(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Ident;

    fn make_eq_expr(col: &str, val: i32) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(col))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                val.to_string(),
                false,
            ))),
        }
    }

    #[test]
    fn test_analyze_predicates_simple_eq() {
        let expr = make_eq_expr("id", 42);
        let predicates = analyze_predicates(&expr);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column, "id");
        assert_eq!(predicates[0].op, PredicateOp::Eq);
    }

    #[test]
    fn test_analyze_predicates_and() {
        let expr = Expr::BinaryOp {
            left: Box::new(make_eq_expr("a", 1)),
            op: BinaryOperator::And,
            right: Box::new(make_eq_expr("b", 2)),
        };
        let predicates = analyze_predicates(&expr);
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn test_choose_full_scan_no_index() {
        let schema = TableSchema {
            name: "test".to_string(),
            table_id: 1,
            columns: vec![],
            version: 1,
            pk_indices: vec![],
            indexes: vec![],
        };
        let path = choose_best_access_path(&schema, &[], 1000);
        assert!(matches!(path.scan_type, ScanType::FullTableScan));
    }

    #[test]
    fn test_choose_index_scan() {
        let schema = TableSchema {
            name: "test".to_string(),
            table_id: 1,
            columns: vec![],
            version: 1,
            pk_indices: vec![],
            indexes: vec![IndexDef {
                id: 1,
                name: "idx_a".to_string(),
                columns: vec!["a".to_string()],
                unique: false,
            }],
        };
        let predicates = vec![PredicateInfo {
            column: "a".to_string(),
            op: PredicateOp::Eq,
            value: Value::Int32(1),
        }];
        let path = choose_best_access_path(&schema, &predicates, 1000);
        assert!(matches!(path.scan_type, ScanType::IndexScan { .. }));
    }
}
