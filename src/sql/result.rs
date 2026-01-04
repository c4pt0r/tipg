//! SQL execution result types

use crate::types::{Row, TableSchema};

/// Result of executing a SQL statement
#[derive(Debug)]
pub enum ExecuteResult {
    /// SELECT result with rows
    Select {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
    /// CREATE TABLE result
    CreateTable { table_name: String },
    /// DROP TABLE result  
    DropTable { table_name: String },
    /// TRUNCATE TABLE result
    TruncateTable { table_name: String },
    /// ALTER TABLE result
    AlterTable { table_name: String },
    /// CREATE INDEX result
    CreateIndex { index_name: String },

    /// INSERT result with affected row count
    Insert { affected_rows: u64 },
    /// DELETE result with affected row count
    Delete { affected_rows: u64 },
    /// UPDATE result with affected row count
    Update { affected_rows: u64 },
    /// SHOW TABLES result
    ShowTables { tables: Vec<String> },
    /// DESCRIBE table result
    Describe { schema: TableSchema },
    /// Empty result (for unsupported/noop statements)
    Empty,
    /// Skipped statement with warning message
    Skipped { message: String },
}

impl ExecuteResult {
    pub fn affected_rows(&self) -> u64 {
        match self {
            ExecuteResult::Insert { affected_rows } => *affected_rows,
            ExecuteResult::Delete { affected_rows } => *affected_rows,
            ExecuteResult::Update { affected_rows } => *affected_rows,
            _ => 0,
        }
    }

    pub fn is_query(&self) -> bool {
        matches!(
            self,
            ExecuteResult::Select { .. }
                | ExecuteResult::ShowTables { .. }
                | ExecuteResult::Describe { .. }
        )
    }
}
