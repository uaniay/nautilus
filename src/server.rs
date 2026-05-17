use std::net::SocketAddr;

use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::auth::AuthState;
use crate::config::Config;
use crate::rate_limit::RateLimiter;
use crate::session::SessionManager;
use crate::transport::socketio::{self, SioState};
use crate::transport::websocket::{self, AppState};

pub async fn run(config: Config) -> anyhow::Result<()> {
    let auth = AuthState::new(config.auth.clone());
    let rate_limiter = RateLimiter::new(config.rate_limit.max_creates_per_minute);
    let session_mgr = SessionManager::new(config.sessions.clone(), rate_limiter);

    let app_state = AppState {
        auth: auth.clone(),
        session_mgr: session_mgr.clone(),
        sessions_config: config.sessions.clone(),
    };

    let sio_state = SioState {
        auth: auth.clone(),
        session_mgr: session_mgr.clone(),
        sessions_config: config.sessions.clone(),
    };

    let (sio_layer, _io) = socketio::build_layer(sio_state);

    let app = Router::new()
        .route("/", get(index_html))
        .route("/ws", get(websocket::ws_handler))
        .route("/health", get(health))
        .layer(sio_layer)
        .layer(CorsLayer::permissive())
        .with_state(app_state);

    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;

    if let Some(tls) = &config.tls {
        info!("Listening on {} (TLS enabled)", addr);
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
            &tls.cert_path,
            &tls.key_path,
        )
        .await?;

        axum_server::bind_rustls(addr, tls_config)
            .serve(app.into_make_service())
            .await?;
    } else {
        info!("Listening on {}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    }

    info!("Server shut down");
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn index_html() -> impl IntoResponse {
    Html(include_str!("../web/index.html"))
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C handler");
    info!("Shutdown signal received");
}
