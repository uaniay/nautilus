use std::sync::Arc;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rusqlite::Connection;
use tokio::sync::Mutex;

pub type SharedUserStore = Arc<Mutex<UserStore>>;

pub struct UserStore {
    db: Connection,
}

impl UserStore {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
        )?;
        Ok(Self { db })
    }

    pub fn create_user(&self, username: &str, password: &str, role: &str) -> anyhow::Result<()> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?
            .to_string();

        self.db.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?1, ?2, ?3)",
            rusqlite::params![username, hash, role],
        )?;
        Ok(())
    }

    pub fn verify(&self, username: &str, password: &str) -> Option<(String, String)> {
        let result: Result<(String, String), _> = self.db.query_row(
            "SELECT password_hash, role FROM users WHERE username = ?1",
            rusqlite::params![username],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        let (hash_str, role) = result.ok()?;
        let parsed_hash = PasswordHash::new(&hash_str).ok()?;
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .ok()?;

        Some((username.to_string(), role))
    }
}
