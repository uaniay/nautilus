use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::State as AxumState,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::auth::claims::Claims;
use crate::auth::AuthState;
use crate::config::Config;
use crate::rate_limit::RateLimiter;
use crate::session::SessionManager;
use crate::stt::{SttProvider, deepgram::DeepgramProvider};
use crate::transport::socketio::{self, SioState};
use crate::transport::websocket::{self, AppState};
use crate::users::UserStore;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let auth = AuthState::new(config.auth.clone());
    let rate_limiter = RateLimiter::new(config.rate_limit.max_creates_per_minute);
    let session_mgr = SessionManager::new(config.sessions.clone(), rate_limiter);
    let user_store = Arc::new(Mutex::new(UserStore::open(&config.auth.db_path)?));

    let stt_provider: Option<Arc<dyn SttProvider>> = config.stt.as_ref().map(|stt_config| {
        let provider: Arc<dyn SttProvider> = match stt_config.provider.as_str() {
            "deepgram" => Arc::new(DeepgramProvider::new(stt_config)),
            other => {
                panic!("Unknown STT provider: '{}'. Supported: deepgram", other);
            }
        };
        info!(provider = %stt_config.provider, "STT provider configured");
        provider
    });

    let app_state = AppState {
        auth: auth.clone(),
        session_mgr: session_mgr.clone(),
        sessions_config: config.sessions.clone(),
        user_store: user_store.clone(),
        stt_provider,
        stt_config: config.stt.clone(),
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
        .route("/api/login", post(login_handler))
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

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

async fn login_handler(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    let store = state.user_store.lock().await;
    match store.verify(&req.username, &req.password) {
        Some((username, role)) => {
            drop(store);
            match Claims::encode(
                &username,
                &role,
                state.auth.config.token_expiry_hours,
                &state.auth.config.jwt_secret,
            ) {
                Ok(token) => {
                    (StatusCode::OK, Json(serde_json::json!({ "token": token }))).into_response()
                }
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
        None => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid username or password" })),
        )
            .into_response(),
    }
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
