use crate::storage::TikvStore;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

pub struct TikvClientPool {
    pd_endpoints: Vec<String>,
    namespace: Option<String>,
    clients: RwLock<HashMap<String, Arc<TikvStore>>>,
}

impl TikvClientPool {
    pub fn new(pd_endpoints: Vec<String>, namespace: Option<String>) -> Self {
        Self {
            pd_endpoints,
            namespace,
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_client(&self, keyspace: Option<String>) -> Result<Arc<TikvStore>> {
        let key = keyspace.clone().unwrap_or_else(|| "default".to_string());

        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(&key) {
                return Ok(client.clone());
            }
        }

        let mut clients = self.clients.write().await;

        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }

        info!("Creating new TiKV client for keyspace: {}", key);
        let store = TikvStore::new_with_keyspace(
            self.pd_endpoints.clone(),
            self.namespace.clone(),
            keyspace,
        )
        .await?;

        let store = Arc::new(store);
        clients.insert(key, store.clone());
        Ok(store)
    }

    pub async fn client_count(&self) -> usize {
        self.clients.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_creation() {
        let pool = TikvClientPool::new(vec!["127.0.0.1:2379".to_string()], None);
        assert_eq!(pool.client_count().await, 0);
    }
}
