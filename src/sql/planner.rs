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

#[derive(Debug, Clone)]
pub struct JoinTableInfo {
    pub name: String,
    pub alias: String,
    pub estimated_rows: usize,
}

pub fn optimize_join_order(tables: &[JoinTableInfo]) -> Vec<usize> {
    if tables.len() <= 1 {
        return (0..tables.len()).collect();
    }

    let mut indices: Vec<usize> = (0..tables.len()).collect();
    indices.sort_by_key(|&i| tables[i].estimated_rows);
    indices
}

pub fn estimate_join_cost(left_rows: usize, right_rows: usize, selectivity: f64) -> f64 {
    let scan_cost = (left_rows + right_rows) as f64;
    let join_cost = (left_rows * right_rows) as f64 * selectivity;
    scan_cost + join_cost
}

#[derive(Debug, Clone)]
pub struct PushdownResult {
    pub table_predicates: HashMap<String, Vec<PredicateInfo>>,
    pub remaining_predicates: Vec<PredicateInfo>,
}

pub fn pushdown_predicates(
    predicates: &[PredicateInfo],
    table_columns: &HashMap<String, Vec<String>>,
) -> PushdownResult {
    let mut table_predicates: HashMap<String, Vec<PredicateInfo>> = HashMap::new();
    let mut remaining_predicates = Vec::new();

    for pred in predicates {
        let mut pushed = false;
        for (table_name, columns) in table_columns {
            if columns.contains(&pred.column) {
                table_predicates
                    .entry(table_name.clone())
                    .or_default()
                    .push(pred.clone());
                pushed = true;
                break;
            }
        }
        if !pushed {
            remaining_predicates.push(pred.clone());
        }
    }

    PushdownResult {
        table_predicates,
        remaining_predicates,
    }
}

pub fn extract_table_predicates(
    predicates: &[PredicateInfo],
    table_alias: &str,
    table_columns: &[String],
) -> Vec<PredicateInfo> {
    predicates
        .iter()
        .filter(|p| {
            let col_name = if p.column.contains('.') {
                let parts: Vec<&str> = p.column.split('.').collect();
                if parts.len() == 2 && parts[0] == table_alias {
                    parts[1].to_string()
                } else {
                    return false;
                }
            } else {
                p.column.clone()
            };
            table_columns.contains(&col_name)
        })
        .cloned()
        .collect()
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
            check_constraints: vec![],
            foreign_keys: vec![],
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
            check_constraints: vec![],
            foreign_keys: vec![],
        };
        let predicates = vec![PredicateInfo {
            column: "a".to_string(),
            op: PredicateOp::Eq,
            value: Value::Int32(1),
        }];
        let path = choose_best_access_path(&schema, &predicates, 1000);
        assert!(matches!(path.scan_type, ScanType::IndexScan { .. }));
    }

    #[test]
    fn test_optimize_join_order_two_tables() {
        let tables = vec![
            JoinTableInfo {
                name: "small".to_string(),
                alias: "s".to_string(),
                estimated_rows: 100,
            },
            JoinTableInfo {
                name: "large".to_string(),
                alias: "l".to_string(),
                estimated_rows: 10000,
            },
        ];
        let order = optimize_join_order(&tables);
        assert_eq!(order[0], 0);
    }

    #[test]
    fn test_pushdown_predicates() {
        let predicates = vec![
            PredicateInfo {
                column: "a".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(1),
            },
            PredicateInfo {
                column: "b".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(2),
            },
        ];
        let mut table_columns = HashMap::new();
        table_columns.insert("t1".to_string(), vec!["a".to_string()]);
        table_columns.insert("t2".to_string(), vec!["b".to_string()]);

        let result = pushdown_predicates(&predicates, &table_columns);
        assert_eq!(result.table_predicates.get("t1").unwrap().len(), 1);
        assert_eq!(result.table_predicates.get("t2").unwrap().len(), 1);
        assert!(result.remaining_predicates.is_empty());
    }

    #[test]
    fn test_analyze_predicates_comparison_ops() {
        let lt_expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("x"))),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                "10".to_string(),
                false,
            ))),
        };
        let predicates = analyze_predicates(&lt_expr);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].op, PredicateOp::Lt);

        let gt_expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("y"))),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                "5".to_string(),
                false,
            ))),
        };
        let predicates = analyze_predicates(&gt_expr);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].op, PredicateOp::Gt);
    }

    #[test]
    fn test_analyze_predicates_nested() {
        let nested = Expr::Nested(Box::new(make_eq_expr("id", 1)));
        let predicates = analyze_predicates(&nested);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column, "id");
    }

    #[test]
    fn test_analyze_predicates_is_null() {
        let is_null = Expr::IsNull(Box::new(Expr::Identifier(Ident::new("col"))));
        let predicates = analyze_predicates(&is_null);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].op, PredicateOp::IsNull);
    }

    #[test]
    fn test_analyze_predicates_is_not_null() {
        let is_not_null = Expr::IsNotNull(Box::new(Expr::Identifier(Ident::new("col"))));
        let predicates = analyze_predicates(&is_not_null);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].op, PredicateOp::IsNotNull);
    }

    #[test]
    fn test_choose_unique_index_over_non_unique() {
        let schema = TableSchema {
            name: "test".to_string(),
            table_id: 1,
            columns: vec![],
            version: 1,
            pk_indices: vec![],
            indexes: vec![
                IndexDef {
                    id: 1,
                    name: "idx_a".to_string(),
                    columns: vec!["a".to_string()],
                    unique: false,
                },
                IndexDef {
                    id: 2,
                    name: "idx_a_unique".to_string(),
                    columns: vec!["a".to_string()],
                    unique: true,
                },
            ],
            check_constraints: vec![],
            foreign_keys: vec![],
        };
        let predicates = vec![PredicateInfo {
            column: "a".to_string(),
            op: PredicateOp::Eq,
            value: Value::Int32(1),
        }];
        let path = choose_best_access_path(&schema, &predicates, 10000);
        if let ScanType::IndexScan { index_name, .. } = path.scan_type {
            assert_eq!(index_name, "idx_a_unique");
        } else {
            panic!("Expected IndexScan");
        }
    }

    #[test]
    fn test_choose_composite_index() {
        let schema = TableSchema {
            name: "test".to_string(),
            table_id: 1,
            columns: vec![],
            version: 1,
            pk_indices: vec![],
            indexes: vec![
                IndexDef {
                    id: 1,
                    name: "idx_a".to_string(),
                    columns: vec!["a".to_string()],
                    unique: false,
                },
                IndexDef {
                    id: 2,
                    name: "idx_ab".to_string(),
                    columns: vec!["a".to_string(), "b".to_string()],
                    unique: false,
                },
            ],
            check_constraints: vec![],
            foreign_keys: vec![],
        };
        let predicates = vec![
            PredicateInfo {
                column: "a".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(1),
            },
            PredicateInfo {
                column: "b".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(2),
            },
        ];
        let path = choose_best_access_path(&schema, &predicates, 10000);
        if let ScanType::IndexScan {
            index_name, values, ..
        } = path.scan_type
        {
            assert_eq!(index_name, "idx_ab");
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected IndexScan on composite index");
        }
    }

    #[test]
    fn test_index_range_scan_partial_match() {
        let schema = TableSchema {
            name: "test".to_string(),
            table_id: 1,
            columns: vec![],
            version: 1,
            pk_indices: vec![],
            indexes: vec![IndexDef {
                id: 1,
                name: "idx_abc".to_string(),
                columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
                unique: false,
            }],
            check_constraints: vec![],
            foreign_keys: vec![],
        };
        let predicates = vec![PredicateInfo {
            column: "a".to_string(),
            op: PredicateOp::Eq,
            value: Value::Int32(1),
        }];
        let path = choose_best_access_path(&schema, &predicates, 10000);
        match path.scan_type {
            ScanType::IndexRangeScan { prefix_values, .. } => {
                assert_eq!(prefix_values.len(), 1);
            }
            _ => panic!("Expected IndexRangeScan for partial match"),
        }
    }

    #[test]
    fn test_optimize_join_order_three_tables() {
        let tables = vec![
            JoinTableInfo {
                name: "medium".to_string(),
                alias: "m".to_string(),
                estimated_rows: 1000,
            },
            JoinTableInfo {
                name: "large".to_string(),
                alias: "l".to_string(),
                estimated_rows: 100000,
            },
            JoinTableInfo {
                name: "small".to_string(),
                alias: "s".to_string(),
                estimated_rows: 10,
            },
        ];
        let order = optimize_join_order(&tables);
        assert_eq!(order, vec![2, 0, 1]);
    }

    #[test]
    fn test_optimize_join_order_single_table() {
        let tables = vec![JoinTableInfo {
            name: "only".to_string(),
            alias: "o".to_string(),
            estimated_rows: 500,
        }];
        let order = optimize_join_order(&tables);
        assert_eq!(order, vec![0]);
    }

    #[test]
    fn test_optimize_join_order_empty() {
        let tables: Vec<JoinTableInfo> = vec![];
        let order = optimize_join_order(&tables);
        assert!(order.is_empty());
    }

    #[test]
    fn test_estimate_join_cost() {
        let cost = estimate_join_cost(100, 1000, 0.01);
        assert!(cost > 0.0);

        let cost_small = estimate_join_cost(10, 10, 0.1);
        let cost_large = estimate_join_cost(1000, 1000, 0.1);
        assert!(cost_large > cost_small);
    }

    #[test]
    fn test_pushdown_predicates_with_remaining() {
        let predicates = vec![
            PredicateInfo {
                column: "a".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(1),
            },
            PredicateInfo {
                column: "unknown".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(99),
            },
        ];
        let mut table_columns = HashMap::new();
        table_columns.insert("t1".to_string(), vec!["a".to_string()]);

        let result = pushdown_predicates(&predicates, &table_columns);
        assert_eq!(result.table_predicates.get("t1").unwrap().len(), 1);
        assert_eq!(result.remaining_predicates.len(), 1);
        assert_eq!(result.remaining_predicates[0].column, "unknown");
    }

    #[test]
    fn test_pushdown_predicates_empty() {
        let predicates: Vec<PredicateInfo> = vec![];
        let table_columns: HashMap<String, Vec<String>> = HashMap::new();

        let result = pushdown_predicates(&predicates, &table_columns);
        assert!(result.table_predicates.is_empty());
        assert!(result.remaining_predicates.is_empty());
    }

    #[test]
    fn test_extract_table_predicates_with_alias() {
        let predicates = vec![
            PredicateInfo {
                column: "t.col1".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(1),
            },
            PredicateInfo {
                column: "other.col2".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(2),
            },
            PredicateInfo {
                column: "col3".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(3),
            },
        ];
        let table_columns = vec!["col1".to_string(), "col3".to_string()];

        let extracted = extract_table_predicates(&predicates, "t", &table_columns);
        assert_eq!(extracted.len(), 2);
    }

    #[test]
    fn test_extract_index_values_success() {
        let predicates = vec![
            PredicateInfo {
                column: "a".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(1),
            },
            PredicateInfo {
                column: "b".to_string(),
                op: PredicateOp::Eq,
                value: Value::Int32(2),
            },
        ];
        let index_columns = vec!["a".to_string(), "b".to_string()];
        let values = extract_index_values(&predicates, &index_columns);
        assert!(values.is_some());
        assert_eq!(values.unwrap().len(), 2);
    }

    #[test]
    fn test_extract_index_values_missing_column() {
        let predicates = vec![PredicateInfo {
            column: "a".to_string(),
            op: PredicateOp::Eq,
            value: Value::Int32(1),
        }];
        let index_columns = vec!["a".to_string(), "b".to_string()];
        let values = extract_index_values(&predicates, &index_columns);
        assert!(values.is_none());
    }

    #[test]
    fn test_extract_index_values_non_eq_predicate() {
        let predicates = vec![PredicateInfo {
            column: "a".to_string(),
            op: PredicateOp::Lt,
            value: Value::Int32(1),
        }];
        let index_columns = vec!["a".to_string()];
        let values = extract_index_values(&predicates, &index_columns);
        assert!(values.is_none());
    }

    #[test]
    fn test_predicate_op_reversed() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                "10".to_string(),
                false,
            ))),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Identifier(Ident::new("x"))),
        };
        let predicates = analyze_predicates(&expr);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column, "x");
        assert_eq!(predicates[0].op, PredicateOp::Gt);
    }

    #[test]
    fn test_full_table_scan_better_for_high_selectivity() {
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
            check_constraints: vec![],
            foreign_keys: vec![],
        };
        let path_no_pred = choose_best_access_path(&schema, &[], 10);
        assert!(matches!(path_no_pred.scan_type, ScanType::FullTableScan));
    }
}
