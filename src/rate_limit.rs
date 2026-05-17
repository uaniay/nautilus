use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;

#[derive(Clone)]
pub struct RateLimiter {
    windows: Arc<DashMap<String, VecDeque<Instant>>>,
    max_per_minute: u32,
}

impl RateLimiter {
    pub fn new(max_per_minute: u32) -> Self {
        Self {
            windows: Arc::new(DashMap::new()),
            max_per_minute,
        }
    }

    pub fn check(&self, key: &str) -> Result<(), String> {
        let now = Instant::now();
        let window = std::time::Duration::from_secs(60);

        let mut entry = self.windows.entry(key.to_string()).or_default();
        let timestamps = entry.value_mut();

        // Remove expired entries
        while timestamps.front().is_some_and(|t| now.duration_since(*t) > window) {
            timestamps.pop_front();
        }

        if timestamps.len() >= self.max_per_minute as usize {
            return Err(format!(
                "Rate limit exceeded: max {} per minute",
                self.max_per_minute
            ));
        }

        timestamps.push_back(now);
        Ok(())
    }
}
