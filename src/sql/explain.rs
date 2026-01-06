//! EXPLAIN statement - PostgreSQL-compatible query plan output

use std::fmt::Write;

use sqlparser::ast::{Expr, Query, Select, SetExpr, Statement, TableFactor, TableWithJoins};

use super::planner::{analyze_predicates, choose_best_access_path, PredicateInfo, ScanType};
use crate::types::TableSchema;

const DEFAULT_ROW_WIDTH: usize = 40;

#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan {
        table_name: String,
        alias: Option<String>,
        filter: Option<String>,
        cost: PlanCost,
    },
    IndexScan {
        table_name: String,
        alias: Option<String>,
        index_name: String,
        index_cond: Option<String>,
        filter: Option<String>,
        cost: PlanCost,
    },
    NestedLoop {
        join_type: String,
        cost: PlanCost,
        children: Vec<PlanNode>,
    },
    Sort {
        sort_key: Vec<String>,
        cost: PlanCost,
        child: Box<PlanNode>,
    },
    Limit {
        count: usize,
        cost: PlanCost,
        child: Box<PlanNode>,
    },
    Aggregate {
        strategy: String,
        keys: Vec<String>,
        cost: PlanCost,
        child: Box<PlanNode>,
    },
    Result {
        cost: PlanCost,
    },
}

#[derive(Debug, Clone)]
pub struct PlanCost {
    pub startup: f64,
    pub total: f64,
    pub rows: usize,
    pub width: usize,
}

impl Default for PlanCost {
    fn default() -> Self {
        Self {
            startup: 0.0,
            total: 0.0,
            rows: 1,
            width: DEFAULT_ROW_WIDTH,
        }
    }
}

pub fn generate_plan(
    stmt: &Statement,
    schema_lookup: impl Fn(&str) -> Option<TableSchema>,
    row_count_lookup: impl Fn(&str) -> usize,
) -> PlanNode {
    match stmt {
        Statement::Query(query) => generate_query_plan(query, &schema_lookup, &row_count_lookup),
        _ => PlanNode::Result {
            cost: PlanCost::default(),
        },
    }
}

fn generate_query_plan(
    query: &Query,
    schema_lookup: &impl Fn(&str) -> Option<TableSchema>,
    row_count_lookup: &impl Fn(&str) -> usize,
) -> PlanNode {
    let mut plan = match &*query.body {
        SetExpr::Select(select) => generate_select_plan(select, schema_lookup, row_count_lookup),
        _ => PlanNode::Result {
            cost: PlanCost::default(),
        },
    };

    if !query.order_by.is_empty() {
        let sort_keys: Vec<String> = query
            .order_by
            .iter()
            .map(|o| format_expr(&o.expr))
            .collect();
        let child_cost = get_plan_cost(&plan);
        let sort_cost =
            child_cost.total + (child_cost.rows as f64 * (child_cost.rows as f64).log2().max(1.0));
        plan = PlanNode::Sort {
            sort_key: sort_keys,
            cost: PlanCost {
                startup: sort_cost,
                total: sort_cost,
                rows: child_cost.rows,
                width: child_cost.width,
            },
            child: Box::new(plan),
        };
    }

    if let Some(limit_expr) = &query.limit {
        if let Some(limit_val) = extract_limit_value(limit_expr) {
            let child_cost = get_plan_cost(&plan);
            let limited_rows = limit_val.min(child_cost.rows);
            plan = PlanNode::Limit {
                count: limit_val,
                cost: PlanCost {
                    startup: child_cost.startup,
                    total: child_cost.startup + (limited_rows as f64 * 0.01),
                    rows: limited_rows,
                    width: child_cost.width,
                },
                child: Box::new(plan),
            };
        }
    }

    plan
}

fn generate_select_plan(
    select: &Select,
    schema_lookup: &impl Fn(&str) -> Option<TableSchema>,
    row_count_lookup: &impl Fn(&str) -> usize,
) -> PlanNode {
    if select.from.is_empty() {
        return PlanNode::Result {
            cost: PlanCost {
                startup: 0.0,
                total: 0.01,
                rows: 1,
                width: 4,
            },
        };
    }

    let predicates = select
        .selection
        .as_ref()
        .map(|expr| analyze_predicates(expr))
        .unwrap_or_default();

    let mut plan = generate_table_plan(
        &select.from[0],
        &predicates,
        select.selection.as_ref(),
        schema_lookup,
        row_count_lookup,
    );

    if select.from.len() > 1 || !select.from[0].joins.is_empty() {
        let mut children = vec![plan.clone()];

        for join in &select.from[0].joins {
            let join_plan = generate_table_factor_plan(
                &join.relation,
                &[],
                None,
                schema_lookup,
                row_count_lookup,
            );
            children.push(join_plan);
        }

        for table_with_joins in select.from.iter().skip(1) {
            let table_plan =
                generate_table_plan(table_with_joins, &[], None, schema_lookup, row_count_lookup);
            children.push(table_plan);
        }

        if children.len() > 1 {
            let total_rows: usize = children.iter().map(|c| get_plan_cost(c).rows).product();
            let total_cost: f64 = children.iter().map(|c| get_plan_cost(c).total).sum::<f64>()
                + (total_rows as f64 * 0.01);
            plan = PlanNode::NestedLoop {
                join_type: "Inner".to_string(),
                cost: PlanCost {
                    startup: 0.0,
                    total: total_cost,
                    rows: total_rows.max(1),
                    width: DEFAULT_ROW_WIDTH,
                },
                children,
            };
        }
    }

    let group_by_exprs = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs) => exprs.clone(),
        sqlparser::ast::GroupByExpr::All => vec![],
    };
    if !group_by_exprs.is_empty() {
        let keys: Vec<String> = group_by_exprs.iter().map(|e| format_expr(e)).collect();
        let child_cost = get_plan_cost(&plan);
        let agg_rows = (child_cost.rows / 10).max(1);
        plan = PlanNode::Aggregate {
            strategy: "HashAggregate".to_string(),
            keys,
            cost: PlanCost {
                startup: child_cost.total,
                total: child_cost.total + (agg_rows as f64 * 0.1),
                rows: agg_rows,
                width: child_cost.width,
            },
            child: Box::new(plan),
        };
    }

    plan
}

fn generate_table_plan(
    table_with_joins: &TableWithJoins,
    predicates: &[PredicateInfo],
    filter_expr: Option<&Expr>,
    schema_lookup: &impl Fn(&str) -> Option<TableSchema>,
    row_count_lookup: &impl Fn(&str) -> usize,
) -> PlanNode {
    generate_table_factor_plan(
        &table_with_joins.relation,
        predicates,
        filter_expr,
        schema_lookup,
        row_count_lookup,
    )
}

fn generate_table_factor_plan(
    table_factor: &TableFactor,
    predicates: &[PredicateInfo],
    filter_expr: Option<&Expr>,
    schema_lookup: &impl Fn(&str) -> Option<TableSchema>,
    row_count_lookup: &impl Fn(&str) -> usize,
) -> PlanNode {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.0.last().map(|i| i.value.as_str()).unwrap_or("");
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            let estimated_rows = row_count_lookup(table_name);

            if let Some(schema) = schema_lookup(table_name) {
                let access_path = choose_best_access_path(&schema, predicates, estimated_rows);

                match access_path.scan_type {
                    ScanType::IndexScan {
                        index_name,
                        estimated_rows: est_rows,
                        ..
                    } => {
                        let index_cond = predicates
                            .iter()
                            .map(|p| format_predicate(p))
                            .collect::<Vec<_>>()
                            .join(" AND ");
                        PlanNode::IndexScan {
                            table_name: table_name.to_string(),
                            alias: alias_name,
                            index_name,
                            index_cond: if index_cond.is_empty() {
                                None
                            } else {
                                Some(index_cond)
                            },
                            filter: None,
                            cost: PlanCost {
                                startup: 0.15,
                                total: 0.15 + (est_rows as f64 * 0.01),
                                rows: est_rows.max(1),
                                width: estimate_row_width(&schema),
                            },
                        }
                    }
                    ScanType::IndexRangeScan {
                        index_name,
                        estimated_rows: est_rows,
                        ..
                    } => {
                        let index_cond = predicates
                            .iter()
                            .map(|p| format_predicate(p))
                            .collect::<Vec<_>>()
                            .join(" AND ");
                        PlanNode::IndexScan {
                            table_name: table_name.to_string(),
                            alias: alias_name,
                            index_name,
                            index_cond: if index_cond.is_empty() {
                                None
                            } else {
                                Some(index_cond)
                            },
                            filter: None,
                            cost: PlanCost {
                                startup: 0.15,
                                total: 0.15 + (est_rows as f64 * 0.01),
                                rows: est_rows.max(1),
                                width: estimate_row_width(&schema),
                            },
                        }
                    }
                    ScanType::FullTableScan => {
                        let filter = filter_expr.map(|e| format_expr(e));
                        PlanNode::SeqScan {
                            table_name: table_name.to_string(),
                            alias: alias_name,
                            filter,
                            cost: PlanCost {
                                startup: 0.0,
                                total: estimated_rows as f64 * 0.01 + 1.0,
                                rows: estimated_rows.max(1),
                                width: estimate_row_width(&schema),
                            },
                        }
                    }
                }
            } else {
                let filter = filter_expr.map(|e| format_expr(e));
                PlanNode::SeqScan {
                    table_name: table_name.to_string(),
                    alias: alias_name,
                    filter,
                    cost: PlanCost {
                        startup: 0.0,
                        total: estimated_rows as f64 * 0.01 + 1.0,
                        rows: estimated_rows.max(1),
                        width: DEFAULT_ROW_WIDTH,
                    },
                }
            }
        }
        TableFactor::Derived {
            subquery, alias: _, ..
        } => {
            let subquery_plan = generate_query_plan(subquery, schema_lookup, row_count_lookup);
            subquery_plan
        }
        _ => PlanNode::Result {
            cost: PlanCost::default(),
        },
    }
}

fn estimate_row_width(schema: &TableSchema) -> usize {
    schema
        .columns
        .iter()
        .map(|c| c.data_type.estimated_size())
        .sum::<usize>()
        .max(8)
}

fn get_plan_cost(plan: &PlanNode) -> PlanCost {
    match plan {
        PlanNode::SeqScan { cost, .. } => cost.clone(),
        PlanNode::IndexScan { cost, .. } => cost.clone(),
        PlanNode::NestedLoop { cost, .. } => cost.clone(),
        PlanNode::Sort { cost, .. } => cost.clone(),
        PlanNode::Limit { cost, .. } => cost.clone(),
        PlanNode::Aggregate { cost, .. } => cost.clone(),
        PlanNode::Result { cost } => cost.clone(),
    }
}

fn extract_limit_value(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(sqlparser::ast::Value::Number(s, _)) => s.parse().ok(),
        _ => None,
    }
}

fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|i| i.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        Expr::Value(v) => format!("{}", v),
        Expr::BinaryOp { left, op, right } => {
            format!("({} {} {})", format_expr(left), op, format_expr(right))
        }
        Expr::UnaryOp { op, expr } => format!("{} {}", op, format_expr(expr)),
        Expr::IsNull(e) => format!("({} IS NULL)", format_expr(e)),
        Expr::IsNotNull(e) => format!("({} IS NOT NULL)", format_expr(e)),
        Expr::Nested(e) => format!("({})", format_expr(e)),
        Expr::Function(f) => {
            let args: Vec<String> = f.args.iter().map(|a| format!("{}", a)).collect();
            format!("{}({})", f.name, args.join(", "))
        }
        _ => format!("{}", expr),
    }
}

fn format_predicate(pred: &PredicateInfo) -> String {
    let op_str = match pred.op {
        super::planner::PredicateOp::Eq => "=",
        super::planner::PredicateOp::Ne => "<>",
        super::planner::PredicateOp::Lt => "<",
        super::planner::PredicateOp::Le => "<=",
        super::planner::PredicateOp::Gt => ">",
        super::planner::PredicateOp::Ge => ">=",
        super::planner::PredicateOp::Like => "~~",
        super::planner::PredicateOp::In => "= ANY",
        super::planner::PredicateOp::IsNull => "IS NULL",
        super::planner::PredicateOp::IsNotNull => "IS NOT NULL",
    };

    match pred.op {
        super::planner::PredicateOp::IsNull | super::planner::PredicateOp::IsNotNull => {
            format!("({} {})", pred.column, op_str)
        }
        _ => format!("({} {} {})", pred.column, op_str, format_value(&pred.value)),
    }
}

fn format_value(value: &crate::types::Value) -> String {
    match value {
        crate::types::Value::Null => "NULL".to_string(),
        crate::types::Value::Boolean(b) => b.to_string(),
        crate::types::Value::Int32(i) => i.to_string(),
        crate::types::Value::Int64(i) => i.to_string(),
        crate::types::Value::Float64(f) => f.to_string(),
        crate::types::Value::Text(s) => format!("'{}'", s),
        crate::types::Value::Bytes(b) => format!("'\\x{}'", hex::encode(b)),
        crate::types::Value::Timestamp(ts) => format!("'{}'", ts),
        crate::types::Value::Interval(i) => format!("'{}'", i),
        crate::types::Value::Uuid(u) => {
            let uuid = uuid::Uuid::from_bytes(*u);
            format!("'{}'", uuid)
        }
        crate::types::Value::Json(j) => format!("'{}'", j),
        crate::types::Value::Jsonb(j) => format!("'{}'", j),
        crate::types::Value::Array(arr) => {
            let elems: Vec<String> = arr.iter().map(format_value).collect();
            format!("ARRAY[{}]", elems.join(", "))
        }
    }
}

pub fn format_plan_text(plan: &PlanNode, indent: usize) -> String {
    let mut output = String::new();
    format_plan_node(&mut output, plan, indent, true);
    output
}

fn format_plan_node(output: &mut String, plan: &PlanNode, indent: usize, is_first: bool) {
    let prefix = if is_first {
        " ".repeat(indent)
    } else {
        format!("{}->  ", " ".repeat(indent.saturating_sub(4)))
    };

    match plan {
        PlanNode::SeqScan {
            table_name,
            alias,
            filter,
            cost,
        } => {
            let table_display = if let Some(a) = alias {
                format!("{} {}", table_name, a)
            } else {
                table_name.clone()
            };
            writeln!(
                output,
                "{}Seq Scan on {}  (cost={:.2}..{:.2} rows={} width={})",
                prefix, table_display, cost.startup, cost.total, cost.rows, cost.width
            )
            .unwrap();
            if let Some(f) = filter {
                writeln!(output, "{}  Filter: {}", " ".repeat(indent), f).unwrap();
            }
        }
        PlanNode::IndexScan {
            table_name,
            alias,
            index_name,
            index_cond,
            filter,
            cost,
        } => {
            let table_display = if let Some(a) = alias {
                format!("{} {}", table_name, a)
            } else {
                table_name.clone()
            };
            writeln!(
                output,
                "{}Index Scan using {} on {}  (cost={:.2}..{:.2} rows={} width={})",
                prefix, index_name, table_display, cost.startup, cost.total, cost.rows, cost.width
            )
            .unwrap();
            if let Some(cond) = index_cond {
                writeln!(output, "{}  Index Cond: {}", " ".repeat(indent), cond).unwrap();
            }
            if let Some(f) = filter {
                writeln!(output, "{}  Filter: {}", " ".repeat(indent), f).unwrap();
            }
        }
        PlanNode::NestedLoop {
            join_type,
            cost,
            children,
        } => {
            writeln!(
                output,
                "{}Nested Loop {}  (cost={:.2}..{:.2} rows={} width={})",
                prefix, join_type, cost.startup, cost.total, cost.rows, cost.width
            )
            .unwrap();
            for (i, child) in children.iter().enumerate() {
                format_plan_node(output, child, indent + 6, i == 0);
            }
        }
        PlanNode::Sort {
            sort_key,
            cost,
            child,
        } => {
            writeln!(
                output,
                "{}Sort  (cost={:.2}..{:.2} rows={} width={})",
                prefix, cost.startup, cost.total, cost.rows, cost.width
            )
            .unwrap();
            writeln!(
                output,
                "{}  Sort Key: {}",
                " ".repeat(indent),
                sort_key.join(", ")
            )
            .unwrap();
            format_plan_node(output, child, indent + 6, false);
        }
        PlanNode::Limit {
            count: _,
            cost,
            child,
        } => {
            writeln!(
                output,
                "{}Limit  (cost={:.2}..{:.2} rows={} width={})",
                prefix, cost.startup, cost.total, cost.rows, cost.width
            )
            .unwrap();
            format_plan_node(output, child, indent + 6, false);
        }
        PlanNode::Aggregate {
            strategy,
            keys,
            cost,
            child,
        } => {
            let key_display = if keys.is_empty() {
                String::new()
            } else {
                format!("  Group Key: {}", keys.join(", "))
            };
            writeln!(
                output,
                "{}{}  (cost={:.2}..{:.2} rows={} width={})",
                prefix, strategy, cost.startup, cost.total, cost.rows, cost.width
            )
            .unwrap();
            if !key_display.is_empty() {
                writeln!(output, "{}{}", " ".repeat(indent), key_display).unwrap();
            }
            format_plan_node(output, child, indent + 6, false);
        }
        PlanNode::Result { cost } => {
            writeln!(
                output,
                "{}Result  (cost={:.2}..{:.2} rows={} width={})",
                prefix, cost.startup, cost.total, cost.rows, cost.width
            )
            .unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, DataType, IndexDef};

    fn dummy_schema_lookup(name: &str) -> Option<TableSchema> {
        if name == "users" {
            Some(TableSchema {
                name: "users".to_string(),
                table_id: 1,
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        primary_key: true,
                        unique: true,
                        is_serial: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        primary_key: false,
                        unique: false,
                        is_serial: false,
                        default_expr: None,
                    },
                ],
                version: 1,
                pk_indices: vec![0],
                indexes: vec![IndexDef {
                    id: 1,
                    name: "users_pkey".to_string(),
                    columns: vec!["id".to_string()],
                    unique: true,
                }],
            })
        } else {
            None
        }
    }

    fn dummy_row_count(_name: &str) -> usize {
        1000
    }

    #[test]
    fn test_seq_scan_plan() {
        let sql = "SELECT * FROM users WHERE name = 'Alice'";
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();

        let plan = generate_plan(&ast[0], dummy_schema_lookup, dummy_row_count);
        let output = format_plan_text(&plan, 0);

        assert!(output.contains("Seq Scan on users"));
        assert!(output.contains("Filter:"));
    }

    #[test]
    fn test_index_scan_plan() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();

        let plan = generate_plan(&ast[0], dummy_schema_lookup, dummy_row_count);
        let output = format_plan_text(&plan, 0);

        assert!(output.contains("Index Scan using users_pkey on users"));
        assert!(output.contains("Index Cond:"));
    }

    #[test]
    fn test_simple_select_plan() {
        let sql = "SELECT 1";
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();

        let plan = generate_plan(&ast[0], dummy_schema_lookup, dummy_row_count);
        let output = format_plan_text(&plan, 0);

        assert!(output.contains("Result"));
    }
}
