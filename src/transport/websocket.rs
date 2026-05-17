use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::info;

use crate::auth::AuthState;
use crate::protocol::{ClientMessage, ServerMessage};
use crate::session::SessionManager;

#[derive(Deserialize)]
pub struct WsQuery {
    token: Option<String>,
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, query.token, state))
}

async fn handle_socket(socket: WebSocket, token: Option<String>, state: AppState) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender));

    let claims = match authenticate(&token, &mut receiver, &state.auth).await {
        Some(c) => c,
        None => {
            let msg = serde_json::to_string(&ServerMessage::Error {
                message: "Authentication failed".to_string(),
            })
            .unwrap();
            let _ = sender.lock().await.send(Message::Text(msg.into())).await;
            return;
        }
    };

    let user = claims.sub.clone();
    info!(user = %user, "WebSocket client authenticated");

    loop {
        tokio::select! {
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let (response, new_session_id) = handle_client_message(&text, &claims, &state).await;
                        if let Some(resp) = response {
                            let json = serde_json::to_string(&resp).unwrap();
                            if sender.lock().await.send(Message::Text(json.into())).await.is_err() {
                                break;
                            }
                        }
                        // Start output forwarding for newly created sessions
                        if let Some(session_id) = new_session_id {
                            if let Ok(mut rx) = state.session_mgr.subscribe(&session_id) {
                                let sender_clone = sender.clone();
                                let sid = session_id.clone();
                                tokio::spawn(async move {
                                    while let Ok(data) = rx.recv().await {
                                        let text = String::from_utf8_lossy(&data).to_string();
                                        let msg = ServerMessage::Output {
                                            session_id: sid.clone(),
                                            data: text,
                                        };
                                        let json = serde_json::to_string(&msg).unwrap();
                                        if sender_clone.lock().await.send(Message::Text(json.into())).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

async fn authenticate(
    token: &Option<String>,
    receiver: &mut futures_util::stream::SplitStream<WebSocket>,
    auth: &AuthState,
) -> Option<crate::auth::claims::Claims> {
    if let Some(t) = token {
        return auth.validate_token(t).ok();
    }

    if let Some(Ok(Message::Text(text))) = receiver.next().await {
        #[derive(Deserialize)]
        struct AuthMsg {
            token: String,
        }
        if let Ok(msg) = serde_json::from_str::<AuthMsg>(&text) {
            return auth.validate_token(&msg.token).ok();
        }
    }

    None
}

async fn handle_client_message(
    text: &str,
    claims: &crate::auth::claims::Claims,
    state: &AppState,
) -> (Option<ServerMessage>, Option<String>) {
    let msg: ClientMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            return (
                Some(ServerMessage::Error {
                    message: format!("Invalid message: {}", e),
                }),
                None,
            );
        }
    };

    match msg {
        ClientMessage::Create {
            command,
            args,
            env,
            cols,
            rows,
        } => {
            if !claims.can_spawn(&command, &state.sessions_config.allowed_commands) {
                return (
                    Some(ServerMessage::Error {
                        message: format!("Not authorized to spawn '{}'", command),
                    }),
                    None,
                );
            }
            match state.session_mgr.create(&claims.sub, &command, args, env, cols, rows) {
                Ok(session_id) => (
                    Some(ServerMessage::Created {
                        session_id: session_id.clone(),
                    }),
                    Some(session_id),
                ),
                Err(e) => (
                    Some(ServerMessage::Error {
                        message: e.to_string(),
                    }),
                    None,
                ),
            }
        }
        ClientMessage::Attach { session_id } => {
            match state.session_mgr.attach(&session_id, &claims.sub) {
                Ok((replay, _rx)) => {
                    let replay_str = String::from_utf8_lossy(&replay).to_string();
                    (
                        Some(ServerMessage::Attached {
                            session_id: session_id.clone(),
                            replay: replay_str,
                        }),
                        Some(session_id),
                    )
                }
                Err(e) => (
                    Some(ServerMessage::Error {
                        message: e.to_string(),
                    }),
                    None,
                ),
            }
        }
        ClientMessage::List => {
            let sessions = state
                .session_mgr
                .list_user_sessions(&claims.sub)
                .into_iter()
                .map(|s| crate::protocol::messages::SessionListEntry {
                    id: s.id,
                    command: s.command,
                    created_at: s.created_at.to_rfc3339(),
                })
                .collect();
            (Some(ServerMessage::SessionList { sessions }), None)
        }
        ClientMessage::Input { session_id, data } => {
            if let Err(e) = state.session_mgr.write(&session_id, data.into_bytes()) {
                return (
                    Some(ServerMessage::Error {
                        message: e.to_string(),
                    }),
                    None,
                );
            }
            (None, None)
        }
        ClientMessage::Resize {
            session_id,
            cols,
            rows,
        } => {
            if let Err(e) = state.session_mgr.resize(&session_id, cols, rows) {
                return (
                    Some(ServerMessage::Error {
                        message: e.to_string(),
                    }),
                    None,
                );
            }
            (None, None)
        }
        ClientMessage::Kill { session_id, .. } => {
            if let Err(e) = state.session_mgr.kill(&session_id) {
                return (
                    Some(ServerMessage::Error {
                        message: e.to_string(),
                    }),
                    None,
                );
            }
            (None, None)
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub auth: AuthState,
    pub session_mgr: SessionManager,
    pub sessions_config: crate::config::SessionsConfig,
}
