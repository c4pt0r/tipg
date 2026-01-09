use crate::storage::TikvStore;
use crate::types::{ColumnDef, DataType, Row, TableSchema, Value};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tikv_client::Transaction;

pub fn is_information_schema_table(table_name: &str) -> bool {
    let lower = table_name.to_lowercase();
    lower.starts_with("information_schema.")
        || matches!(
            lower.as_str(),
            "tables"
                | "columns"
                | "schemata"
                | "table_constraints"
                | "key_column_usage"
                | "referential_constraints"
                | "constraint_column_usage"
                | "check_constraints"
        )
}

pub fn parse_information_schema_table(table_name: &str) -> Option<&str> {
    let lower = table_name.to_lowercase();
    if let Some(name) = lower.strip_prefix("information_schema.") {
        return Some(match name {
            "tables" => "tables",
            "columns" => "columns",
            "schemata" => "schemata",
            "table_constraints" => "table_constraints",
            "key_column_usage" => "key_column_usage",
            "referential_constraints" => "referential_constraints",
            "constraint_column_usage" => "constraint_column_usage",
            "check_constraints" => "check_constraints",
            _ => return None,
        });
    }
    None
}

fn text_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: DataType::Text,
        nullable: true,
        primary_key: false,
        unique: false,
        is_serial: false,
        default_expr: None,
    }
}

fn int_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: DataType::Int64,
        nullable: true,
        primary_key: false,
        unique: false,
        is_serial: false,
        default_expr: None,
    }
}

fn tables_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "tables".to_string(),
        columns: vec![
            text_col("table_catalog"),
            text_col("table_schema"),
            text_col("table_name"),
            text_col("table_type"),
            text_col("self_referencing_column_name"),
            text_col("reference_generation"),
            text_col("user_defined_type_catalog"),
            text_col("user_defined_type_schema"),
            text_col("user_defined_type_name"),
            text_col("is_insertable_into"),
            text_col("is_typed"),
            text_col("commit_action"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

fn columns_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "columns".to_string(),
        columns: vec![
            text_col("table_catalog"),
            text_col("table_schema"),
            text_col("table_name"),
            text_col("column_name"),
            int_col("ordinal_position"),
            text_col("column_default"),
            text_col("is_nullable"),
            text_col("data_type"),
            int_col("character_maximum_length"),
            int_col("character_octet_length"),
            int_col("numeric_precision"),
            int_col("numeric_precision_radix"),
            int_col("numeric_scale"),
            int_col("datetime_precision"),
            text_col("interval_type"),
            int_col("interval_precision"),
            text_col("character_set_catalog"),
            text_col("character_set_schema"),
            text_col("character_set_name"),
            text_col("collation_catalog"),
            text_col("collation_schema"),
            text_col("collation_name"),
            text_col("domain_catalog"),
            text_col("domain_schema"),
            text_col("domain_name"),
            text_col("udt_catalog"),
            text_col("udt_schema"),
            text_col("udt_name"),
            text_col("scope_catalog"),
            text_col("scope_schema"),
            text_col("scope_name"),
            int_col("maximum_cardinality"),
            text_col("dtd_identifier"),
            text_col("is_self_referencing"),
            text_col("is_identity"),
            text_col("identity_generation"),
            text_col("identity_start"),
            text_col("identity_increment"),
            text_col("identity_maximum"),
            text_col("identity_minimum"),
            text_col("identity_cycle"),
            text_col("is_generated"),
            text_col("generation_expression"),
            text_col("is_updatable"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

fn schemata_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "schemata".to_string(),
        columns: vec![
            text_col("catalog_name"),
            text_col("schema_name"),
            text_col("schema_owner"),
            text_col("default_character_set_catalog"),
            text_col("default_character_set_schema"),
            text_col("default_character_set_name"),
            text_col("sql_path"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

fn table_constraints_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "table_constraints".to_string(),
        columns: vec![
            text_col("constraint_catalog"),
            text_col("constraint_schema"),
            text_col("constraint_name"),
            text_col("table_catalog"),
            text_col("table_schema"),
            text_col("table_name"),
            text_col("constraint_type"),
            text_col("is_deferrable"),
            text_col("initially_deferred"),
            text_col("enforced"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

fn key_column_usage_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "key_column_usage".to_string(),
        columns: vec![
            text_col("constraint_catalog"),
            text_col("constraint_schema"),
            text_col("constraint_name"),
            text_col("table_catalog"),
            text_col("table_schema"),
            text_col("table_name"),
            text_col("column_name"),
            int_col("ordinal_position"),
            int_col("position_in_unique_constraint"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

fn referential_constraints_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "referential_constraints".to_string(),
        columns: vec![
            text_col("constraint_catalog"),
            text_col("constraint_schema"),
            text_col("constraint_name"),
            text_col("unique_constraint_catalog"),
            text_col("unique_constraint_schema"),
            text_col("unique_constraint_name"),
            text_col("match_option"),
            text_col("update_rule"),
            text_col("delete_rule"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

fn constraint_column_usage_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "constraint_column_usage".to_string(),
        columns: vec![
            text_col("table_catalog"),
            text_col("table_schema"),
            text_col("table_name"),
            text_col("column_name"),
            text_col("constraint_catalog"),
            text_col("constraint_schema"),
            text_col("constraint_name"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

fn check_constraints_schema() -> TableSchema {
    TableSchema {
        table_id: 0,
        name: "check_constraints".to_string(),
        columns: vec![
            text_col("constraint_catalog"),
            text_col("constraint_schema"),
            text_col("constraint_name"),
            text_col("check_clause"),
        ],
        version: 1,
        pk_indices: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

pub fn get_information_schema_schema(table_name: &str) -> Option<TableSchema> {
    let lower = table_name.to_lowercase();
    let name = lower.strip_prefix("information_schema.").unwrap_or(&lower);

    match name {
        "tables" => Some(tables_schema()),
        "columns" => Some(columns_schema()),
        "schemata" => Some(schemata_schema()),
        "table_constraints" => Some(table_constraints_schema()),
        "key_column_usage" => Some(key_column_usage_schema()),
        "referential_constraints" => Some(referential_constraints_schema()),
        "constraint_column_usage" => Some(constraint_column_usage_schema()),
        "check_constraints" => Some(check_constraints_schema()),
        _ => None,
    }
}

fn data_type_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "boolean",
        DataType::Int32 => "integer",
        DataType::Int64 => "bigint",
        DataType::Float64 => "double precision",
        DataType::Text => "text",
        DataType::Bytes => "bytea",
        DataType::Timestamp => "timestamp without time zone",
        DataType::Interval => "interval",
        DataType::Uuid => "uuid",
        DataType::Array(inner) => match inner.as_ref() {
            DataType::Int32 => "integer[]",
            DataType::Int64 => "bigint[]",
            DataType::Text => "text[]",
            _ => "anyarray",
        },
        DataType::Json => "json",
        DataType::Jsonb => "jsonb",
        DataType::Vector(_) => "vector",
    }
}

fn text_val(s: &str) -> Value {
    Value::Text(s.to_string())
}

fn null_val() -> Value {
    Value::Null
}

fn int_val(i: i64) -> Value {
    Value::Int64(i)
}

pub async fn get_information_schema_data(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    table_name: &str,
) -> Result<(TableSchema, Vec<Row>)> {
    let lower = table_name.to_lowercase();
    let name = lower.strip_prefix("information_schema.").unwrap_or(&lower);

    let schema =
        get_information_schema_schema(table_name).ok_or_else(|| anyhow!("Unknown table"))?;

    let user_tables = store.list_tables(txn).await?;

    let rows = match name {
        "schemata" => get_schemata_rows(),
        "tables" => get_tables_rows(store, txn, &user_tables).await?,
        "columns" => get_columns_rows(store, txn, &user_tables).await?,
        "table_constraints" => get_table_constraints_rows(store, txn, &user_tables).await?,
        "key_column_usage" => get_key_column_usage_rows(store, txn, &user_tables).await?,
        "referential_constraints" => {
            get_referential_constraints_rows(store, txn, &user_tables).await?
        }
        "constraint_column_usage" => {
            get_constraint_column_usage_rows(store, txn, &user_tables).await?
        }
        "check_constraints" => get_check_constraints_rows(store, txn, &user_tables).await?,
        _ => vec![],
    };

    Ok((schema, rows))
}

fn get_schemata_rows() -> Vec<Row> {
    vec![
        Row::new(vec![
            text_val("postgres"),
            text_val("public"),
            text_val("postgres"),
            null_val(),
            null_val(),
            null_val(),
            null_val(),
        ]),
        Row::new(vec![
            text_val("postgres"),
            text_val("information_schema"),
            text_val("postgres"),
            null_val(),
            null_val(),
            null_val(),
            null_val(),
        ]),
        Row::new(vec![
            text_val("postgres"),
            text_val("pg_catalog"),
            text_val("postgres"),
            null_val(),
            null_val(),
            null_val(),
            null_val(),
        ]),
    ]
}

async fn get_tables_rows(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    user_tables: &[String],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();

    for table_name in user_tables {
        rows.push(Row::new(vec![
            text_val("postgres"),
            text_val("public"),
            text_val(table_name),
            text_val("BASE TABLE"),
            null_val(),
            null_val(),
            null_val(),
            null_val(),
            null_val(),
            text_val("YES"),
            text_val("NO"),
            null_val(),
        ]));
    }

    let views = store.list_views(txn).await.unwrap_or_default();
    for view_name in views {
        rows.push(Row::new(vec![
            text_val("postgres"),
            text_val("public"),
            text_val(&view_name),
            text_val("VIEW"),
            null_val(),
            null_val(),
            null_val(),
            null_val(),
            null_val(),
            text_val("NO"),
            text_val("NO"),
            null_val(),
        ]));
    }

    Ok(rows)
}

async fn get_columns_rows(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    user_tables: &[String],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();

    for table_name in user_tables {
        if let Some(schema) = store.get_schema(txn, table_name).await? {
            for (i, col) in schema.columns.iter().enumerate() {
                let pg_type = data_type_to_pg_type(&col.data_type);
                let is_nullable = if col.nullable { "YES" } else { "NO" };
                let ordinal = (i + 1) as i64;

                let (char_max_len, num_precision, num_scale) = match col.data_type {
                    DataType::Int32 => (null_val(), int_val(32), int_val(0)),
                    DataType::Int64 => (null_val(), int_val(64), int_val(0)),
                    DataType::Float64 => (null_val(), int_val(53), null_val()),
                    DataType::Text => (null_val(), null_val(), null_val()),
                    _ => (null_val(), null_val(), null_val()),
                };

                rows.push(Row::new(vec![
                    text_val("postgres"),
                    text_val("public"),
                    text_val(table_name),
                    text_val(&col.name),
                    int_val(ordinal),
                    col.default_expr
                        .as_ref()
                        .map(|s| text_val(s))
                        .unwrap_or(null_val()),
                    text_val(is_nullable),
                    text_val(pg_type),
                    char_max_len,
                    null_val(),
                    num_precision,
                    int_val(2),
                    num_scale,
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    text_val("postgres"),
                    text_val("pg_catalog"),
                    text_val(pg_type),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    text_val(&ordinal.to_string()),
                    text_val("NO"),
                    text_val("NO"),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    null_val(),
                    text_val("NEVER"),
                    null_val(),
                    text_val("YES"),
                ]));
            }
        }
    }

    Ok(rows)
}

async fn get_table_constraints_rows(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    user_tables: &[String],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();

    for table_name in user_tables {
        if let Some(schema) = store.get_schema(txn, table_name).await? {
            if !schema.pk_indices.is_empty() {
                let pk_name = format!("{}_pkey", table_name);
                rows.push(Row::new(vec![
                    text_val("postgres"),
                    text_val("public"),
                    text_val(&pk_name),
                    text_val("postgres"),
                    text_val("public"),
                    text_val(table_name),
                    text_val("PRIMARY KEY"),
                    text_val("NO"),
                    text_val("NO"),
                    text_val("YES"),
                ]));
            }

            for idx in &schema.indexes {
                if idx.unique {
                    rows.push(Row::new(vec![
                        text_val("postgres"),
                        text_val("public"),
                        text_val(&idx.name),
                        text_val("postgres"),
                        text_val("public"),
                        text_val(table_name),
                        text_val("UNIQUE"),
                        text_val("NO"),
                        text_val("NO"),
                        text_val("YES"),
                    ]));
                }
            }

            for col in &schema.columns {
                if col.unique && !col.primary_key {
                    let constraint_name = format!("{}_{}_key", table_name, col.name);
                    rows.push(Row::new(vec![
                        text_val("postgres"),
                        text_val("public"),
                        text_val(&constraint_name),
                        text_val("postgres"),
                        text_val("public"),
                        text_val(table_name),
                        text_val("UNIQUE"),
                        text_val("NO"),
                        text_val("NO"),
                        text_val("YES"),
                    ]));
                }
            }

            for fk in &schema.foreign_keys {
                rows.push(Row::new(vec![
                    text_val("postgres"),
                    text_val("public"),
                    text_val(&fk.name),
                    text_val("postgres"),
                    text_val("public"),
                    text_val(table_name),
                    text_val("FOREIGN KEY"),
                    text_val("NO"),
                    text_val("NO"),
                    text_val("YES"),
                ]));
            }

            for (i, check) in schema.check_constraints.iter().enumerate() {
                let name = check
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_check{}", table_name, i + 1));
                rows.push(Row::new(vec![
                    text_val("postgres"),
                    text_val("public"),
                    text_val(&name),
                    text_val("postgres"),
                    text_val("public"),
                    text_val(table_name),
                    text_val("CHECK"),
                    text_val("NO"),
                    text_val("NO"),
                    text_val("YES"),
                ]));
            }
        }
    }

    Ok(rows)
}

async fn get_key_column_usage_rows(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    user_tables: &[String],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();

    for table_name in user_tables {
        if let Some(schema) = store.get_schema(txn, table_name).await? {
            if !schema.pk_indices.is_empty() {
                let pk_name = format!("{}_pkey", table_name);
                for (i, &col_idx) in schema.pk_indices.iter().enumerate() {
                    let col_name = &schema.columns[col_idx].name;
                    rows.push(Row::new(vec![
                        text_val("postgres"),
                        text_val("public"),
                        text_val(&pk_name),
                        text_val("postgres"),
                        text_val("public"),
                        text_val(table_name),
                        text_val(col_name),
                        int_val((i + 1) as i64),
                        null_val(),
                    ]));
                }
            }

            for idx in &schema.indexes {
                if idx.unique {
                    for (i, col_name) in idx.columns.iter().enumerate() {
                        rows.push(Row::new(vec![
                            text_val("postgres"),
                            text_val("public"),
                            text_val(&idx.name),
                            text_val("postgres"),
                            text_val("public"),
                            text_val(table_name),
                            text_val(col_name),
                            int_val((i + 1) as i64),
                            null_val(),
                        ]));
                    }
                }
            }

            for fk in &schema.foreign_keys {
                for (i, col_name) in fk.columns.iter().enumerate() {
                    rows.push(Row::new(vec![
                        text_val("postgres"),
                        text_val("public"),
                        text_val(&fk.name),
                        text_val("postgres"),
                        text_val("public"),
                        text_val(table_name),
                        text_val(col_name),
                        int_val((i + 1) as i64),
                        int_val((i + 1) as i64),
                    ]));
                }
            }
        }
    }

    Ok(rows)
}

async fn get_referential_constraints_rows(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    user_tables: &[String],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();

    for table_name in user_tables {
        if let Some(schema) = store.get_schema(txn, table_name).await? {
            for fk in &schema.foreign_keys {
                let ref_pk_name = format!("{}_pkey", fk.ref_table);
                let update_rule = match fk.on_update {
                    crate::types::ForeignKeyAction::Cascade => "CASCADE",
                    crate::types::ForeignKeyAction::SetNull => "SET NULL",
                    crate::types::ForeignKeyAction::SetDefault => "SET DEFAULT",
                    crate::types::ForeignKeyAction::Restrict => "RESTRICT",
                    crate::types::ForeignKeyAction::NoAction => "NO ACTION",
                };
                let delete_rule = match fk.on_delete {
                    crate::types::ForeignKeyAction::Cascade => "CASCADE",
                    crate::types::ForeignKeyAction::SetNull => "SET NULL",
                    crate::types::ForeignKeyAction::SetDefault => "SET DEFAULT",
                    crate::types::ForeignKeyAction::Restrict => "RESTRICT",
                    crate::types::ForeignKeyAction::NoAction => "NO ACTION",
                };
                rows.push(Row::new(vec![
                    text_val("postgres"),
                    text_val("public"),
                    text_val(&fk.name),
                    text_val("postgres"),
                    text_val("public"),
                    text_val(&ref_pk_name),
                    text_val("NONE"),
                    text_val(update_rule),
                    text_val(delete_rule),
                ]));
            }
        }
    }

    Ok(rows)
}

async fn get_constraint_column_usage_rows(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    user_tables: &[String],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();

    for table_name in user_tables {
        if let Some(schema) = store.get_schema(txn, table_name).await? {
            if !schema.pk_indices.is_empty() {
                let pk_name = format!("{}_pkey", table_name);
                for &col_idx in &schema.pk_indices {
                    let col_name = &schema.columns[col_idx].name;
                    rows.push(Row::new(vec![
                        text_val("postgres"),
                        text_val("public"),
                        text_val(table_name),
                        text_val(col_name),
                        text_val("postgres"),
                        text_val("public"),
                        text_val(&pk_name),
                    ]));
                }
            }

            for idx in &schema.indexes {
                if idx.unique {
                    for col_name in &idx.columns {
                        rows.push(Row::new(vec![
                            text_val("postgres"),
                            text_val("public"),
                            text_val(table_name),
                            text_val(col_name),
                            text_val("postgres"),
                            text_val("public"),
                            text_val(&idx.name),
                        ]));
                    }
                }
            }

            for fk in &schema.foreign_keys {
                for col_name in &fk.ref_columns {
                    rows.push(Row::new(vec![
                        text_val("postgres"),
                        text_val("public"),
                        text_val(&fk.ref_table),
                        text_val(col_name),
                        text_val("postgres"),
                        text_val("public"),
                        text_val(&fk.name),
                    ]));
                }
            }
        }
    }

    Ok(rows)
}

async fn get_check_constraints_rows(
    store: &Arc<TikvStore>,
    txn: &mut Transaction,
    user_tables: &[String],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();

    for table_name in user_tables {
        if let Some(schema) = store.get_schema(txn, table_name).await? {
            for (i, check) in schema.check_constraints.iter().enumerate() {
                let name = check
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_check{}", table_name, i + 1));
                rows.push(Row::new(vec![
                    text_val("postgres"),
                    text_val("public"),
                    text_val(&name),
                    text_val(&check.expr),
                ]));
            }
        }
    }

    Ok(rows)
}
