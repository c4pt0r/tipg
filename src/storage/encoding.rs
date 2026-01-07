//! Key encoding for TiKV
//!
//! Key layout:
//! - `_sys_next_table_id` -> u64 (auto-incrementing table ID)
//! - `_sys_schema_{table_name}` -> TableSchema (serialized)
//! - `t_{table_id}_{row_key}` -> Row (serialized)
//! - `i_{table_id}_{index_id}_{index_values}` -> PK (Unique Index)
//! - `i_{table_id}_{index_id}_{index_values}_{pk}` -> Empty (Non-Unique Index)

use crate::types::{Row, TableSchema, Value};
use anyhow::{Context, Result};

/// System key prefixes
const SYS_NEXT_TABLE_ID: &[u8] = b"_sys_next_table_id";
const SYS_SCHEMA_PREFIX: &[u8] = b"_sys_schema_";
const SYS_VIEW_PREFIX: &[u8] = b"_sys_view_";
const TABLE_DATA_PREFIX: &[u8] = b"t_";
const TABLE_INDEX_PREFIX: &[u8] = b"i_";

/// Apply namespace prefix to a key
/// Format: `n_{namespace}_{original_key}`
pub fn apply_namespace(namespace: &str, key: &[u8]) -> Vec<u8> {
    if namespace.is_empty() {
        return key.to_vec();
    }
    let mut namespaced = Vec::with_capacity(2 + namespace.len() + 1 + key.len());
    namespaced.extend_from_slice(b"n_");
    namespaced.extend_from_slice(namespace.as_bytes());
    namespaced.push(b'_');
    namespaced.extend_from_slice(key);
    namespaced
}

/// Strip namespace prefix from a key (if present)
pub fn strip_namespace<'a>(namespace: &str, key: &'a [u8]) -> &'a [u8] {
    if namespace.is_empty() {
        return key;
    }
    let prefix_len = 2 + namespace.len() + 1; // "n_" + namespace + "_"
    if key.len() >= prefix_len && &key[0..2] == b"n_" {
        &key[prefix_len..]
    } else {
        key
    }
}

/// Encode the system key for next table ID
pub fn encode_next_table_id_key() -> Vec<u8> {
    SYS_NEXT_TABLE_ID.to_vec()
}

/// Encode the schema key for a table
pub fn encode_schema_key(table_name: &str) -> Vec<u8> {
    let mut key = SYS_SCHEMA_PREFIX.to_vec();
    key.extend_from_slice(table_name.as_bytes());
    key
}

pub fn encode_view_key(view_name: &str) -> Vec<u8> {
    let mut key = SYS_VIEW_PREFIX.to_vec();
    key.extend_from_slice(view_name.as_bytes());
    key
}

pub fn encode_view_prefix() -> Vec<u8> {
    SYS_VIEW_PREFIX.to_vec()
}

/// Encode a data key for a row
pub fn encode_data_key(table_id: u64, row_key: &[u8]) -> Vec<u8> {
    let mut key = TABLE_DATA_PREFIX.to_vec();
    key.extend_from_slice(&table_id.to_be_bytes());
    key.push(b'_');
    key.extend_from_slice(row_key);
    key
}

/// Encode an index key
/// If pk is None, it's a unique index key (Value -> PK)
/// If pk is Some, it's a non-unique index key (Value+PK -> Empty)
pub fn encode_index_key(
    table_id: u64,
    index_id: u64,
    values: &[Value],
    pk: Option<&[Value]>,
) -> Vec<u8> {
    let mut key = TABLE_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&table_id.to_be_bytes());
    key.push(b'_');
    key.extend_from_slice(&index_id.to_be_bytes());
    key.push(b'_');
    key.extend_from_slice(&bincode::serialize(values).unwrap_or_default());

    if let Some(pk_values) = pk {
        key.push(b'_');
        key.extend_from_slice(&bincode::serialize(pk_values).unwrap_or_default());
    }
    key
}

/// Decode PK from a non-unique index key
/// Key: prefix ... {values} _ {pk}
/// This is hard because we don't know length of {values}.
/// BUT for TPC-C, we only use this for scanning.
/// If we scan with prefix `i_..._{values}_`, the remaining part IS `{pk}`.
pub fn decode_index_pk_from_key(full_key: &[u8], prefix_len: usize) -> Result<Vec<Value>> {
    let pk_bytes = &full_key[prefix_len..];
    bincode::deserialize(pk_bytes).context("Failed to deserialize PK from index key")
}

/// Get the key range for scanning all rows of a table
pub fn encode_table_data_range(table_id: u64) -> (Vec<u8>, Vec<u8>) {
    let mut start = TABLE_DATA_PREFIX.to_vec();
    start.extend_from_slice(&table_id.to_be_bytes());
    start.push(b'_');

    let mut end = TABLE_DATA_PREFIX.to_vec();
    end.extend_from_slice(&(table_id + 1).to_be_bytes());

    (start, end)
}

/// Get the raw prefix for schema keys (for scanning all tables)
pub fn encode_schema_prefix() -> Vec<u8> {
    SYS_SCHEMA_PREFIX.to_vec()
}

/// Encode primary key values (composite) to bytes
pub fn encode_pk_values(values: &[Value]) -> Vec<u8> {
    // Serialize the whole vector of values
    bincode::serialize(values).unwrap_or_default()
}

/// Serialize a table schema
pub fn serialize_schema(schema: &TableSchema) -> Result<Vec<u8>> {
    bincode::serialize(schema).context("Failed to serialize schema")
}

/// Deserialize a table schema
pub fn deserialize_schema(data: &[u8]) -> Result<TableSchema> {
    bincode::deserialize(data).context("Failed to deserialize schema")
}

/// Serialize a row
pub fn serialize_row(row: &Row) -> Result<Vec<u8>> {
    bincode::serialize(row).context("Failed to serialize row")
}

/// Deserialize a row
pub fn deserialize_row(data: &[u8]) -> Result<Row> {
    bincode::deserialize(data).context("Failed to deserialize row")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, DataType};

    #[test]
    fn test_apply_namespace_empty() {
        let key = b"test_key";
        let result = apply_namespace("", key);
        assert_eq!(result, key.to_vec());
    }

    #[test]
    fn test_apply_namespace_with_value() {
        let key = b"test_key";
        let result = apply_namespace("myns", key);
        assert_eq!(result, b"n_myns_test_key".to_vec());
    }

    #[test]
    fn test_strip_namespace_empty() {
        let key = b"test_key";
        let result = strip_namespace("", key);
        assert_eq!(result, key);
    }

    #[test]
    fn test_strip_namespace_with_value() {
        let key = b"n_myns_test_key";
        let result = strip_namespace("myns", key);
        assert_eq!(result, b"test_key");
    }

    #[test]
    fn test_encode_schema_key() {
        let key = encode_schema_key("users");
        assert_eq!(key, b"_sys_schema_users".to_vec());
    }

    #[test]
    fn test_encode_data_key() {
        let pk = encode_pk_values(&[Value::Int32(42)]);
        let key = encode_data_key(1, &pk);
        assert!(key.starts_with(b"t_"));
    }

    #[test]
    fn test_encode_table_data_range() {
        let (start, end) = encode_table_data_range(5);
        assert!(start.starts_with(b"t_"));
        assert!(end.starts_with(b"t_"));
        assert!(start < end);
    }

    #[test]
    fn test_encode_pk_values_single() {
        let values = vec![Value::Int32(42)];
        let encoded = encode_pk_values(&values);
        let decoded: Vec<Value> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_encode_pk_values_composite() {
        let values = vec![Value::Int32(1), Value::Text("test".to_string())];
        let encoded = encode_pk_values(&values);
        let decoded: Vec<Value> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_serialize_deserialize_row() {
        let row = Row::new(vec![
            Value::Int32(1),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
            Value::Null,
        ]);
        let serialized = serialize_row(&row).unwrap();
        let deserialized = deserialize_row(&serialized).unwrap();
        assert_eq!(deserialized.values, row.values);
    }

    #[test]
    fn test_serialize_deserialize_schema() {
        let schema = TableSchema {
            name: "test_table".to_string(),
            table_id: 42,
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    primary_key: true,
                    unique: false,
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
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![],
        };
        let serialized = serialize_schema(&schema).unwrap();
        let deserialized = deserialize_schema(&serialized).unwrap();
        assert_eq!(deserialized.name, schema.name);
        assert_eq!(deserialized.table_id, schema.table_id);
        assert_eq!(deserialized.columns.len(), 2);
    }

    #[test]
    fn test_encode_index_key_unique() {
        let values = vec![Value::Int32(1)];
        let key = encode_index_key(1, 2, &values, None);
        assert!(key.starts_with(b"i_"));
    }

    #[test]
    fn test_encode_index_key_non_unique() {
        let values = vec![Value::Int32(1)];
        let pk = vec![Value::Int32(100)];
        let key = encode_index_key(1, 2, &values, Some(&pk));
        assert!(key.starts_with(b"i_"));
        assert!(key.len() > encode_index_key(1, 2, &values, None).len());
    }

    #[test]
    fn test_namespace_roundtrip() {
        let original = b"my_key_data";
        let ns = "test_namespace";
        let namespaced = apply_namespace(ns, original);
        let stripped = strip_namespace(ns, &namespaced);
        assert_eq!(stripped, original);
    }
}
