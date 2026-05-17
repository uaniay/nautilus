pub mod pty;
pub mod types;

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use tracing::info;

use crate::config::SessionsConfig;
use crate::rate_limit::RateLimiter;
use crate::session::pty::PtyProcess;
use crate::session::types::{SessionHandle, SessionInfo, ReplayBuffer};

#[derive(Clone)]
pub struct SessionManager {
    sessions: Arc<DashMap<String, Arc<SessionHandle>>>,
    config: SessionsConfig,
    rate_limiter: RateLimiter,
}

impl SessionManager {
    pub fn new(config: SessionsConfig, rate_limiter: RateLimiter) -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            config,
            rate_limiter,
        }
    }

    pub fn create(
        &self,
        user: &str,
        command: &str,
        args: Vec<String>,
        env: HashMap<String, String>,
        cols: Option<u16>,
        rows: Option<u16>,
    ) -> anyhow::Result<String> {
        if !self.config.allowed_commands.contains(&command.to_string()) {
            anyhow::bail!("Command '{}' is not in the allowed list", command);
        }

        self.rate_limiter
            .check(user)
            .map_err(|e| anyhow::anyhow!(e))?;

        let user_session_count = self
            .sessions
            .iter()
            .filter(|entry| entry.value().info.user == user)
            .count();

        if user_session_count >= self.config.max_sessions_per_user {
            anyhow::bail!(
                "Maximum sessions ({}) reached for user '{}'",
                self.config.max_sessions_per_user,
                user
            );
        }

        let cols = cols.unwrap_or(self.config.default_cols);
        let rows = rows.unwrap_or(self.config.default_rows);

        let info = SessionInfo::new(user, command, args, cols, rows);
        let session_id = info.id.clone();

        let process = PtyProcess::spawn(info.clone(), env.clone())?;

        let handle = Arc::new(SessionHandle {
            info,
            writer: process.input_tx,
            output_tx: process.output_tx,
            resize_tx: process.resize_tx,
            replay: std::sync::Mutex::new(ReplayBuffer::new(self.config.replay_buffer_size)),
        });

        // Spawn replay buffer writer
        let handle_clone = handle.clone();
        let mut replay_rx = handle.output_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(data) = replay_rx.recv().await {
                if let Ok(mut replay) = handle_clone.replay.lock() {
                    replay.push(&data);
                }
            }
        });

        self.sessions.insert(session_id.clone(), handle);
        info!(session_id = %session_id, command, user, "Session created");

        Ok(session_id)
    }

    pub fn write(&self, session_id: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let handle = self
            .sessions
            .get(session_id)
            .ok_or_else(|| anyhow::anyhow!("Session '{}' not found", session_id))?;

        handle
            .writer
            .try_send(data)
            .map_err(|_| anyhow::anyhow!("Failed to write to session"))?;

        Ok(())
    }

    pub fn resize(&self, session_id: &str, cols: u16, rows: u16) -> anyhow::Result<()> {
        let handle = self
            .sessions
            .get(session_id)
            .ok_or_else(|| anyhow::anyhow!("Session '{}' not found", session_id))?;

        handle
            .resize_tx
            .try_send((cols, rows))
            .map_err(|_| anyhow::anyhow!("Failed to resize session"))?;

        Ok(())
    }

    pub fn subscribe(&self, session_id: &str) -> anyhow::Result<broadcast::Receiver<Vec<u8>>> {
        let handle = self
            .sessions
            .get(session_id)
            .ok_or_else(|| anyhow::anyhow!("Session '{}' not found", session_id))?;

        Ok(handle.output_tx.subscribe())
    }

    pub fn attach(&self, session_id: &str, user: &str) -> anyhow::Result<(Vec<u8>, broadcast::Receiver<Vec<u8>>)> {
        let handle = self
            .sessions
            .get(session_id)
            .ok_or_else(|| anyhow::anyhow!("Session '{}' not found", session_id))?;

        if handle.info.user != user {
            anyhow::bail!("Session '{}' does not belong to user '{}'", session_id, user);
        }

        let replay = handle.replay.lock().unwrap().snapshot();
        let rx = handle.output_tx.subscribe();

        Ok((replay, rx))
    }

    pub fn kill(&self, session_id: &str) -> anyhow::Result<()> {
        if self.sessions.remove(session_id).is_some() {
            info!(session_id, "Session removed");
            Ok(())
        } else {
            anyhow::bail!("Session '{}' not found", session_id)
        }
    }

    pub fn list_user_sessions(&self, user: &str) -> Vec<SessionInfo> {
        self.sessions
            .iter()
            .filter(|entry| entry.value().info.user == user)
            .map(|entry| entry.value().info.clone())
            .collect()
    }
}
