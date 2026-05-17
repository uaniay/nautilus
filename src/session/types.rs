use std::collections::VecDeque;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: String,
    pub user: String,
    pub command: String,
    pub args: Vec<String>,
    pub cols: u16,
    pub rows: u16,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl SessionInfo {
    pub fn new(user: &str, command: &str, args: Vec<String>, cols: u16, rows: u16) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            user: user.to_string(),
            command: command.to_string(),
            args,
            cols,
            rows,
            created_at: chrono::Utc::now(),
        }
    }
}

pub struct ReplayBuffer {
    buf: VecDeque<u8>,
    capacity: usize,
}

impl ReplayBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, data: &[u8]) {
        for &byte in data {
            if self.buf.len() >= self.capacity {
                self.buf.pop_front();
            }
            self.buf.push_back(byte);
        }
    }

    pub fn snapshot(&self) -> Vec<u8> {
        self.buf.iter().copied().collect()
    }
}

pub struct SessionHandle {
    pub info: SessionInfo,
    pub writer: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub output_tx: tokio::sync::broadcast::Sender<Vec<u8>>,
    pub resize_tx: tokio::sync::mpsc::Sender<(u16, u16)>,
    pub replay: Mutex<ReplayBuffer>,
}
