use std::{env, error::Error, net::SocketAddr, path::PathBuf, sync::Arc};

use server::ChatRoom;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("server=info,tower_http=info")),
        )
        .init();

    let addr: SocketAddr = env::var("ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".into())
        .parse()?;
    let static_dir = env::var("STATIC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../client/dist"));

    let app = server::app(Arc::new(ChatRoom::default()), &static_dir);
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "ouvindo em http://{addr} (estáticos de {})",
        static_dir.display()
    );
    axum::serve(listener, app).await?;
    Ok(())
}
