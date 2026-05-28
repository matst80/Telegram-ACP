use tokio::time::{sleep_until, Duration, Instant};

pub struct OutboundThrottle {
    min_interval: Duration,
    next_allowed_at: Instant,
}

impl OutboundThrottle {
    pub fn with_interval(secs: f64) -> Self {
        let min_interval = Duration::from_secs_f64(secs);
        Self {
            min_interval,
            next_allowed_at: Instant::now(),
        }
    }

    pub async fn wait_turn(&mut self) {
        let now = Instant::now();
        if self.next_allowed_at > now {
            sleep_until(self.next_allowed_at).await;
        }
        self.next_allowed_at = Instant::now() + self.min_interval;
    }

    pub fn try_turn(&mut self) -> bool {
        let now = Instant::now();
        if self.next_allowed_at > now {
            return false;
        }
        self.next_allowed_at = now + self.min_interval;
        true
    }

    pub fn defer_for(&mut self, delay: Duration) {
        let retry_at = Instant::now() + delay;
        if retry_at > self.next_allowed_at {
            self.next_allowed_at = retry_at;
        }
    }
}