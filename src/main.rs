//! The `kinference` server binary. Everything lives in the library; see
//! [`kinference::server::run`].

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("a rustls crypto provider is already installed"))?;
    kinference::server::run().await
}
