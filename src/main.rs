//! pg-tikv: A PostgreSQL-compatible SQL layer on TiKV

mod protocol;
mod sql;
mod storage;
mod types;

use anyhow::Result;
use pgwire::tokio::process_socket;
use protocol::HandlerFactory;
use sql::Executor;
use std::env;
use std::sync::Arc;
use storage::TikvStore;
use tokio::net::TcpListener;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

const DEFAULT_PG_PORT: u16 = 5433;
const DEFAULT_PD_ENDPOINTS: &str = "127.0.0.1:2379";

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let pd_endpoints = env::var("PD_ENDPOINTS").unwrap_or_else(|_| DEFAULT_PD_ENDPOINTS.to_string());
    let pg_port: u16 = env::var("PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PG_PORT);
    let namespace = env::var("PG_NAMESPACE").ok();

    info!("pg-tikv starting up...");
    info!("PD endpoints: {}", pd_endpoints);
    info!("PostgreSQL port: {}", pg_port);
    if let Some(ns) = &namespace {
        info!("Namespace: {}", ns);
    } else {
        info!("Namespace: (default/global)");
    }

    let pd_addrs: Vec<String> = pd_endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let store = Arc::new(TikvStore::new(pd_addrs, namespace).await?);
    let executor = Arc::new(Executor::new(store));

    let addr = format!("0.0.0.0:{}", pg_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("PostgreSQL server listening on {}", addr);
    info!("Connect using: psql -h 127.0.0.1 -p {}", pg_port);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        info!("New connection from {}", peer_addr);

        let exec = executor.clone();

        tokio::spawn(async move {
            let factory = HandlerFactory::new(exec);
            if let Err(e) = process_socket(socket, None, factory).await {
                tracing::error!("Connection error: {}", e);
            }
            info!("Connection closed: {}", peer_addr);
        });
    }
}
