use std::{env, error::Error, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use server::{ChatRoom, Heartbeat, Shutdown};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

/// Quanto tempo esperar os WebSockets fecharem depois do sinal de encerramento.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

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

    let shutdown = Shutdown::default();
    let app = server::app(
        Arc::new(ChatRoom::default()),
        &static_dir,
        Heartbeat::default(),
        shutdown.clone(),
    );
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "ouvindo em http://{addr} (estáticos de {})",
        static_dir.display()
    );

    axum::serve(listener, app)
        .with_graceful_shutdown({
            let shutdown = shutdown.clone();
            async move {
                shutdown_signal().await;
                tracing::info!("sinal de encerramento recebido; fechando as conexões");
                shutdown.trigger();
            }
        })
        .await?;

    // O axum já parou de aceitar conexões; falta esperar os WebSockets mandarem o `Close`.
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, shutdown.closed())
        .await
        .is_err()
    {
        tracing::warn!("conexões não fecharam em {SHUTDOWN_TIMEOUT:?}; saindo assim mesmo");
    }
    tracing::info!("servidor encerrado");
    Ok(())
}

/// Completa no Ctrl+C ou, em Unix, no SIGTERM (o que o Docker e o systemd mandam).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("falha ao instalar o handler de Ctrl+C");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("falha ao instalar o handler de SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
