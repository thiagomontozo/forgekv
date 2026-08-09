use std::time::SystemTime;

use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    value: Bytes,
    expires_at: Option<SystemTime>,
}

impl Entry {
    pub fn new(value: Bytes, expires_at: Option<SystemTime>) -> Self {
        Self { value, expires_at }
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }

    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    pub fn clear_expiration(&mut self) {
        self.expires_at = None;
    }
}
