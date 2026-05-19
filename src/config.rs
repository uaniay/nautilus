use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub sessions: SessionsConfig,
    pub rate_limit: RateLimitConfig,
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub jwt_secret: String,
    pub token_expiry_hours: u64,
    #[serde(default = "default_db_path")]
    pub db_path: String,
}

fn default_db_path() -> String {
    "nautilus.db".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionsConfig {
    pub max_sessions_per_user: usize,
    pub allowed_commands: Vec<String>,
    pub default_cols: u16,
    pub default_rows: u16,
    #[serde(default = "default_replay_buffer_size")]
    pub replay_buffer_size: usize,
}

fn default_replay_buffer_size() -> usize {
    65536
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    pub max_creates_per_minute: u32,
}

impl Config {
    pub fn load(path: &PathBuf) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
