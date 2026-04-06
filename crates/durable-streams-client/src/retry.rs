use rand::Rng;
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
        self.jittered_backoff_for_attempt(attempt, &mut rand::rng())
    }

    #[must_use]
    pub fn jittered_backoff_for_attempt<R: Rng + ?Sized>(
        &self,
        attempt: usize,
        rng: &mut R,
    ) -> Duration {
        let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
        let multiplier = 2u32.saturating_pow(exponent);
        let backoff = self.initial_backoff.saturating_mul(multiplier);
        let capped = backoff.min(self.max_backoff);
        let jitter_factor = rng.random_range(0.5_f64..=1.0_f64);
        capped.mul_f64(jitter_factor)
    }
}

#[cfg(test)]
mod tests {
    use super::RetryPolicy;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::time::Duration;

    #[test]
    fn jittered_backoff_stays_within_expected_bounds() {
        let policy = RetryPolicy {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        };
        let mut rng = StdRng::seed_from_u64(7);

        let backoff = policy.jittered_backoff_for_attempt(3, &mut rng);
        assert!(backoff >= Duration::from_millis(200));
        assert!(backoff <= Duration::from_millis(400));
    }
}
