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
    current_user: Option<String>,
    is_superuser: bool,
}

impl Session {
    pub fn new(store: Arc<TikvStore>) -> Self {
        Self {
            store,
            state: TransactionState::Idle,
            current_user: None,
            is_superuser: false,
        }
    }

    pub fn new_with_user(store: Arc<TikvStore>, username: String, is_superuser: bool) -> Self {
        Self {
            store,
            state: TransactionState::Idle,
            current_user: Some(username),
            is_superuser,
        }
    }

    pub fn store(&self) -> Arc<TikvStore> {
        self.store.clone()
    }

    pub fn current_user(&self) -> Option<&str> {
        self.current_user.as_deref()
    }

    pub fn is_superuser(&self) -> bool {
        self.is_superuser
    }

    pub fn set_user(&mut self, username: String, is_superuser: bool) {
        self.current_user = Some(username);
        self.is_superuser = is_superuser;
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
            TransactionState::Active(mut txn) => txn.rollback().await.map_err(|e| anyhow!(e)),
            TransactionState::Idle => {
                Ok(()) // No-op
            }
        }
    }
}
