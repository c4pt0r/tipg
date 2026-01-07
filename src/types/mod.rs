//! Data types for the SQL engine

use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported column data types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Float64,
    Text,
    Bytes,
    Timestamp,
    Interval,
    Uuid,
    Array(Box<DataType>),
    // NOTE: Json and Jsonb MUST remain at end of enum to preserve bincode compatibility
    // with existing serialized schemas. Do not reorder!
    Json,
    Jsonb,
}

impl DataType {
    pub fn estimated_size(&self) -> usize {
        match self {
            DataType::Boolean => 1,
            DataType::Int32 => 4,
            DataType::Int64 => 8,
            DataType::Float64 => 8,
            DataType::Text => 32,
            DataType::Bytes => 32,
            DataType::Timestamp => 8,
            DataType::Interval => 8,
            DataType::Uuid => 16,
            DataType::Array(_) => 64,
            DataType::Json => 64,
            DataType::Jsonb => 64,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int32 => write!(f, "INT"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Bytes => write!(f, "BYTEA"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Array(elem_type) => write!(f, "{}[]", elem_type),
            DataType::Json => write!(f, "JSON"),
            DataType::Jsonb => write!(f, "JSONB"),
        }
    }
}

/// A single value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
    Interval(i64),
    Uuid([u8; 16]),
    Array(Vec<Value>),
    Json(String),
    Jsonb(String),
}

impl Value {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Int32(_) => Some(DataType::Int32),
            Value::Int64(_) => Some(DataType::Int64),
            Value::Float64(_) => Some(DataType::Float64),
            Value::Text(_) => Some(DataType::Text),
            Value::Bytes(_) => Some(DataType::Bytes),
            Value::Timestamp(_) => Some(DataType::Timestamp),
            Value::Interval(_) => Some(DataType::Interval),
            Value::Uuid(_) => Some(DataType::Uuid),
            Value::Array(elems) => {
                let elem_type = elems.first().and_then(|v| v.data_type());
                Some(DataType::Array(Box::new(
                    elem_type.unwrap_or(DataType::Text),
                )))
            }
            Value::Json(_) => Some(DataType::Json),
            Value::Jsonb(_) => Some(DataType::Jsonb),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Int32(i) => write!(f, "{}", i),
            Value::Int64(i) => write!(f, "{}", i),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Text(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "{:?}", b),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::Interval(ms) => {
                let days = *ms / (1000 * 60 * 60 * 24);
                let hours = (*ms % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
                let mins = (*ms % (1000 * 60 * 60)) / (1000 * 60);
                let secs = (*ms % (1000 * 60)) / 1000;
                if days > 0 {
                    write!(f, "{} days {:02}:{:02}:{:02}", days, hours, mins, secs)
                } else {
                    write!(f, "{:02}:{:02}:{:02}", hours, mins, secs)
                }
            }
            Value::Uuid(bytes) => {
                write!(
                    f,
                    "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                    u16::from_be_bytes([bytes[4], bytes[5]]),
                    u16::from_be_bytes([bytes[6], bytes[7]]),
                    u16::from_be_bytes([bytes[8], bytes[9]]),
                    u64::from_be_bytes([
                        0, 0, bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
                    ])
                )
            }
            Value::Array(elems) => {
                write!(f, "{{")?;
                for (i, elem) in elems.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    match elem {
                        Value::Text(s) => write!(f, "\"{}\"", s.replace('"', "\\\""))?,
                        v => write!(f, "{}", v)?,
                    }
                }
                write!(f, "}}")
            }
            Value::Json(s) => write!(f, "{}", s),
            Value::Jsonb(s) => write!(f, "{}", s),
        }
    }
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub is_serial: bool,
    pub default_expr: Option<String>,
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub id: u64,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub name: String,
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum ForeignKeyAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub table_id: u64,
    pub columns: Vec<ColumnDef>,
    pub version: u64,
    pub pk_indices: Vec<usize>,
    pub indexes: Vec<IndexDef>,
    #[serde(default)]
    pub check_constraints: Vec<CheckConstraint>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyConstraint>,
}

impl TableSchema {
    pub fn new(
        name: String,
        table_id: u64,
        columns: Vec<ColumnDef>,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            name,
            table_id,
            columns,
            version: 1,
            pk_indices,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }
}

impl TableSchema {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn get_pk_values(&self, row: &Row) -> Vec<Value> {
        if self.pk_indices.is_empty() {
            vec![Value::Uuid(*uuid::Uuid::new_v4().as_bytes())]
        } else {
            self.pk_indices
                .iter()
                .map(|&idx| row.values[idx].clone())
                .collect()
        }
    }

    // Helper to get Index values
    pub fn get_index_values<'a>(&self, index: &IndexDef, row: &'a Row) -> Vec<Value> {
        let mut values = Vec::new();
        for col_name in &index.columns {
            if let Some(idx) = self.column_index(col_name) {
                values.push(row.values[idx].clone());
            } else {
                // Should not happen if index validated
                values.push(Value::Null);
            }
        }
        values
    }
}

/// A row of data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}
