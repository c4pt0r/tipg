//! Session management for transactions

use crate::storage::TikvStore;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tikv_client::Transaction;

pub enum TransactionState {
    Idle,
    Active(Transaction),
}

pub struct Session {
    store: Arc<TikvStore>,
    state: TransactionState,
}

impl Session {
    pub fn new(store: Arc<TikvStore>) -> Self {
        Self {
            store,
            state: TransactionState::Idle,
        }
    }

    pub fn store(&self) -> Arc<TikvStore> {
        self.store.clone()
    }

    /// Check if currently in a transaction block
    pub fn is_in_transaction(&self) -> bool {
        matches!(self.state, TransactionState::Active(_))
    }

    /// Get mutable reference to active transaction
    pub fn get_mut_txn(&mut self) -> Option<&mut Transaction> {
        match &mut self.state {
            TransactionState::Active(txn) => Some(txn),
            _ => None,
        }
    }

    /// Start a transaction block (BEGIN)
    pub async fn begin(&mut self) -> Result<()> {
        match self.state {
            TransactionState::Idle => {
                let txn = self.store.begin().await?;
                self.state = TransactionState::Active(txn);
                Ok(())
            }
            TransactionState::Active(_) => {
                // Already in transaction, ignore
                Ok(())
            }
        }
    }

    /// Commit a transaction block (COMMIT)
    pub async fn commit(&mut self) -> Result<()> {
        // Move txn out of state to take ownership
        match std::mem::replace(&mut self.state, TransactionState::Idle) {
            TransactionState::Active(mut txn) => {
                txn.commit().await.map(|_| ()).map_err(|e| anyhow!(e))
            }
            TransactionState::Idle => {
                Ok(()) // No-op
            }
        }
    }

    /// Rollback a transaction block (ROLLBACK)
    pub async fn rollback(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, TransactionState::Idle) {
            TransactionState::Active(mut txn) => {
                txn.rollback().await.map_err(|e| anyhow!(e))
            }
            TransactionState::Idle => {
                Ok(()) // No-op
            }
        }
    }
}
