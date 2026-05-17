use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub role: String,
    pub exp: u64,
    pub iat: u64,
}

impl Claims {
    pub fn encode(
        sub: &str,
        role: &str,
        expiry_hours: u64,
        secret: &str,
    ) -> Result<String, jsonwebtoken::errors::Error> {
        let now = chrono::Utc::now().timestamp() as u64;
        let claims = Claims {
            sub: sub.to_string(),
            role: role.to_string(),
            iat: now,
            exp: now + expiry_hours * 3600,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
    }

    pub fn decode(token: &str, secret: &str) -> Result<Self, jsonwebtoken::errors::Error> {
        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &Validation::default(),
        )?;
        Ok(data.claims)
    }

    pub fn can_spawn(&self, command: &str, allowed: &[String]) -> bool {
        if self.role == "admin" {
            return true;
        }
        allowed.iter().any(|c| c == command)
    }
}
