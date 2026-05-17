use socketioxide::extract::{Data, SocketRef, State};
use socketioxide::SocketIo;
use tracing::{info, warn};

use crate::auth::AuthState;
use crate::config::SessionsConfig;
use crate::session::SessionManager;

#[derive(Clone)]
pub struct SioState {
    pub auth: AuthState,
    pub session_mgr: SessionManager,
    pub sessions_config: SessionsConfig,
}

pub fn build_layer(state: SioState) -> (socketioxide::layer::SocketIoLayer, SocketIo) {
    let (layer, io) = SocketIo::builder()
        .with_state(state)
        .build_layer();

    io.ns("/cli", on_connect);

    (layer, io)
}

async fn on_connect(socket: SocketRef, State(state): State<SioState>) {
    let token = socket
        .req_parts()
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
        .or_else(|| {
            socket
                .req_parts()
                .uri
                .query()
                .and_then(|q| {
                    url::form_urlencoded::parse(q.as_bytes())
                        .find(|(k, _)| k == "token")
                        .map(|(_, v)| v.to_string())
                })
        });

    let claims = match token.and_then(|t| state.auth.validate_token(&t).ok()) {
        Some(c) => c,
        None => {
            warn!(sid = %socket.id, "Socket.IO auth failed");
            let _ = socket.emit("error", &serde_json::json!({"message": "Authentication failed"}));
            socket.disconnect().ok();
            return;
        }
    };

    let user = claims.sub.clone();
    info!(sid = %socket.id, user = %user, "Socket.IO client connected");

    socket.extensions.insert(claims);

    socket.on("session:create", |socket: SocketRef, Data(data): Data<serde_json::Value>, State(state): State<SioState>| async move {
        let claims = match socket.extensions.get::<crate::auth::claims::Claims>() {
            Some(c) => c.clone(),
            None => return,
        };

        let command = data.get("command").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let args: Vec<String> = data.get("args")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let env: std::collections::HashMap<String, String> = data.get("env")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let cols = data.get("cols").and_then(|v| v.as_u64()).map(|v| v as u16);
        let rows = data.get("rows").and_then(|v| v.as_u64()).map(|v| v as u16);

        if !claims.can_spawn(&command, &state.sessions_config.allowed_commands) {
            let _ = socket.emit("error", &serde_json::json!({"message": format!("Not authorized to spawn '{}'", command)}));
            return;
        }

        match state.session_mgr.create(&claims.sub, &command, args, env, cols, rows) {
            Ok(session_id) => {
                let _ = socket.join(session_id.clone());

                if let Ok(mut rx) = state.session_mgr.subscribe(&session_id) {
                    let socket_clone = socket.clone();
                    let sid = session_id.clone();
                    tokio::spawn(async move {
                        while let Ok(data) = rx.recv().await {
                            let text = String::from_utf8_lossy(&data).to_string();
                            if socket_clone.emit("session:output", &serde_json::json!({
                                "session_id": sid,
                                "data": text
                            })).is_err() {
                                break;
                            }
                        }
                    });
                }

                let _ = socket.emit("session:created", &serde_json::json!({"session_id": session_id}));
            }
            Err(e) => {
                let _ = socket.emit("error", &serde_json::json!({"message": e.to_string()}));
            }
        }
    });

    socket.on("session:attach", |socket: SocketRef, Data(data): Data<serde_json::Value>, State(state): State<SioState>| async move {
        let claims = match socket.extensions.get::<crate::auth::claims::Claims>() {
            Some(c) => c.clone(),
            None => return,
        };

        let session_id = data.get("session_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();

        match state.session_mgr.attach(&session_id, &claims.sub) {
            Ok((replay, mut rx)) => {
                let replay_str = String::from_utf8_lossy(&replay).to_string();
                let _ = socket.emit("session:attached", &serde_json::json!({
                    "session_id": session_id,
                    "replay": replay_str
                }));

                let _ = socket.join(session_id.clone());

                let socket_clone = socket.clone();
                let sid = session_id.clone();
                tokio::spawn(async move {
                    while let Ok(data) = rx.recv().await {
                        let text = String::from_utf8_lossy(&data).to_string();
                        if socket_clone.emit("session:output", &serde_json::json!({
                            "session_id": sid,
                            "data": text
                        })).is_err() {
                            break;
                        }
                    }
                });
            }
            Err(e) => {
                let _ = socket.emit("error", &serde_json::json!({"message": e.to_string()}));
            }
        }
    });

    socket.on("session:list", |socket: SocketRef, State(state): State<SioState>| async move {
        let claims = match socket.extensions.get::<crate::auth::claims::Claims>() {
            Some(c) => c.clone(),
            None => return,
        };

        let sessions: Vec<serde_json::Value> = state
            .session_mgr
            .list_user_sessions(&claims.sub)
            .into_iter()
            .map(|s| serde_json::json!({
                "id": s.id,
                "command": s.command,
                "created_at": s.created_at.to_rfc3339()
            }))
            .collect();

        let _ = socket.emit("session:list", &serde_json::json!({"sessions": sessions}));
    });

    socket.on("session:input", |socket: SocketRef, Data(data): Data<serde_json::Value>, State(state): State<SioState>| async move {
        let session_id = data.get("session_id").and_then(|v| v.as_str()).unwrap_or_default();
        let input = data.get("data").and_then(|v| v.as_str()).unwrap_or_default();

        if let Err(e) = state.session_mgr.write(session_id, input.as_bytes().to_vec()) {
            let _ = socket.emit("error", &serde_json::json!({"message": e.to_string()}));
        }
    });

    socket.on("session:resize", |socket: SocketRef, Data(data): Data<serde_json::Value>, State(state): State<SioState>| async move {
        let session_id = data.get("session_id").and_then(|v| v.as_str()).unwrap_or_default();
        let cols = data.get("cols").and_then(|v| v.as_u64()).unwrap_or(80) as u16;
        let rows = data.get("rows").and_then(|v| v.as_u64()).unwrap_or(24) as u16;

        if let Err(e) = state.session_mgr.resize(session_id, cols, rows) {
            let _ = socket.emit("error", &serde_json::json!({"message": e.to_string()}));
        }
    });

    socket.on("session:kill", |socket: SocketRef, Data(data): Data<serde_json::Value>, State(state): State<SioState>| async move {
        let session_id = data.get("session_id").and_then(|v| v.as_str()).unwrap_or_default();

        match state.session_mgr.kill(session_id) {
            Ok(_) => {
                let _ = socket.emit("session:exit", &serde_json::json!({"session_id": session_id, "code": -1}));
            }
            Err(e) => {
                let _ = socket.emit("error", &serde_json::json!({"message": e.to_string()}));
            }
        }
    });
}
