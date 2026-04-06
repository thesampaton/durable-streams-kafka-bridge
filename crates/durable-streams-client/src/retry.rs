use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    #[must_use]
    pub fn backoff_for_attempt(&self, attempt: usize) -> Duration {
        let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
        let multiplier = 2u32.saturating_pow(exponent);
        let backoff = self.initial_backoff.saturating_mul(multiplier);
        backoff.min(self.max_backoff)
    }
}
