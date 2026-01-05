use anyhow::{anyhow, Context, Result};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

pub fn setup_tls(cert_path: &str, key_path: &str) -> Result<TlsAcceptor> {
    let cert_path = Path::new(cert_path);
    let key_path = Path::new(key_path);

    let cert_file = File::open(cert_path)
        .with_context(|| format!("Failed to open certificate file: {}", cert_path.display()))?;
    let certs: Vec<CertificateDer> = certs(&mut BufReader::new(cert_file))
        .collect::<std::io::Result<Vec<_>>>()
        .context("Failed to parse certificate")?;

    if certs.is_empty() {
        return Err(anyhow!("No certificates found in {}", cert_path.display()));
    }

    let key_file = File::open(key_path)
        .with_context(|| format!("Failed to open key file: {}", key_path.display()))?;
    let mut key_reader = BufReader::new(key_file);

    let key = load_private_key(&mut key_reader, key_path)?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("Failed to build TLS config")?;

    config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_private_key(reader: &mut BufReader<File>, path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pkcs8_keys: Vec<PrivateKeyDer> = pkcs8_private_keys(reader)
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<std::io::Result<Vec<_>>>()
        .unwrap_or_default();

    if !pkcs8_keys.is_empty() {
        return Ok(pkcs8_keys.into_iter().next().unwrap());
    }

    let key_file = File::open(path)?;
    let mut reader = BufReader::new(key_file);
    let rsa_keys: Vec<PrivateKeyDer> = rsa_private_keys(&mut reader)
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<std::io::Result<Vec<_>>>()
        .unwrap_or_default();

    if !rsa_keys.is_empty() {
        return Ok(rsa_keys.into_iter().next().unwrap());
    }

    Err(anyhow!(
        "No valid private key found in {}. Supports PKCS#8 and RSA formats.",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_tls_missing_cert() {
        let result = setup_tls("/nonexistent/cert.pem", "/nonexistent/key.pem");
        assert!(result.is_err());
    }
}
