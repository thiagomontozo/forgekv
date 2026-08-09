mod entry;
mod shard;

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;

use crate::{error::StoreError, metrics::Metrics};

pub use entry::Entry;
use shard::Shard;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TtlState {
    Missing,
    Persistent,
    ExpiresIn(Duration),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub expires_at: Option<SystemTime>,
}

#[derive(Debug)]
pub struct ShardedStore {
    shards: Vec<Shard>,
    metrics: Arc<Metrics>,
}

impl ShardedStore {
    pub fn new(shard_count: usize, metrics: Arc<Metrics>) -> Result<Self, StoreError> {
        if shard_count == 0 {
            return Err(StoreError::InvalidShardCount);
        }
        let shards = (0..shard_count).map(|_| Shard::default()).collect();
        Ok(Self { shards, metrics })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_index(&self, key: &[u8]) -> usize {
        (fnv1a(key) % self.shards.len() as u64) as usize
    }

    pub fn set(&self, key: Bytes, value: Bytes) -> Result<(), StoreError> {
        self.set_with_expiry(key, value, None)
    }

    pub fn set_with_ttl(
        &self,
        key: Bytes,
        value: Bytes,
        ttl: Duration,
    ) -> Result<SystemTime, StoreError> {
        if ttl.is_zero() {
            return Err(StoreError::InvalidTtl);
        }
        let expires_at = SystemTime::now()
            .checked_add(ttl)
            .ok_or(StoreError::InvalidExpiration)?;
        self.set_with_expiry(key, value, Some(expires_at))?;
        Ok(expires_at)
    }

    pub fn set_with_expiry(
        &self,
        key: Bytes,
        value: Bytes,
        expires_at: Option<SystemTime>,
    ) -> Result<(), StoreError> {
        let shard = &self.shards[self.shard_index(&key)];
        let mut entries = shard.write()?;
        entries.insert(key.to_vec(), Entry::new(value, expires_at));
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, StoreError> {
        self.metrics.get();
        let shard = &self.shards[self.shard_index(key)];
        let now = SystemTime::now();
        {
            let entries = shard.read()?;
            match entries.get(key) {
                Some(entry) if !entry.is_expired_at(now) => {
                    self.metrics.hit();
                    return Ok(Some(entry.value().clone()));
                }
                None => {
                    self.metrics.miss();
                    return Ok(None);
                }
                Some(_) => {}
            }
        }
        if remove_if_expired(shard, key, now)? {
            self.metrics.expired(1);
        }
        self.metrics.miss();
        Ok(None)
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, StoreError> {
        let shard = &self.shards[self.shard_index(key)];
        let now = SystemTime::now();
        {
            let entries = shard.read()?;
            match entries.get(key) {
                Some(entry) if !entry.is_expired_at(now) => return Ok(true),
                None => return Ok(false),
                Some(_) => {}
            }
        }
        if remove_if_expired(shard, key, now)? {
            self.metrics.expired(1);
        }
        Ok(false)
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool, StoreError> {
        let shard = &self.shards[self.shard_index(key)];
        let now = SystemTime::now();
        let mut entries = shard.write()?;
        match entries.remove(key) {
            Some(entry) if entry.is_expired_at(now) => {
                self.metrics.expired(1);
                Ok(false)
            }
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn persist(&self, key: &[u8]) -> Result<bool, StoreError> {
        let shard = &self.shards[self.shard_index(key)];
        let now = SystemTime::now();
        let mut entries = shard.write()?;
        if entries
            .get(key)
            .is_some_and(|entry| entry.is_expired_at(now))
        {
            entries.remove(key);
            self.metrics.expired(1);
            return Ok(false);
        }
        match entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at().is_some() {
                    entry.clear_expiration();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => Ok(false),
        }
    }

    pub fn ttl(&self, key: &[u8]) -> Result<TtlState, StoreError> {
        let shard = &self.shards[self.shard_index(key)];
        let now = SystemTime::now();
        {
            let entries = shard.read()?;
            match entries.get(key) {
                None => return Ok(TtlState::Missing),
                Some(entry) if entry.is_expired_at(now) => {}
                Some(entry) => {
                    return Ok(match entry.expires_at() {
                        Some(expires_at) => TtlState::ExpiresIn(
                            expires_at.duration_since(now).unwrap_or(Duration::ZERO),
                        ),
                        None => TtlState::Persistent,
                    })
                }
            }
        }
        if remove_if_expired(shard, key, now)? {
            self.metrics.expired(1);
        }
        Ok(TtlState::Missing)
    }

    pub fn purge_expired(&self) -> Result<u64, StoreError> {
        let now = SystemTime::now();
        let mut removed = 0u64;
        for shard in &self.shards {
            let mut entries = shard.write()?;
            let before = entries.len();
            entries.retain(|_, entry| !entry.is_expired_at(now));
            let count = before.saturating_sub(entries.len()) as u64;
            removed = removed.saturating_add(count);
        }
        if removed > 0 {
            self.metrics.expired(removed);
        }
        Ok(removed)
    }

    pub fn len(&self) -> Result<usize, StoreError> {
        self.purge_expired()?;
        self.shards.iter().try_fold(0usize, |total, shard| {
            let length = shard.read()?.len();
            total
                .checked_add(length)
                .ok_or(StoreError::CapacityOverflow)
        })
    }

    pub fn is_empty(&self) -> Result<bool, StoreError> {
        Ok(self.len()? == 0)
    }

    pub fn snapshot_entries(&self) -> Result<Vec<SnapshotEntry>, StoreError> {
        let now = SystemTime::now();
        let mut snapshot = Vec::new();
        for shard in &self.shards {
            let entries = shard.read()?;
            for (key, entry) in entries.iter() {
                if !entry.is_expired_at(now) {
                    snapshot.push(SnapshotEntry {
                        key: Bytes::copy_from_slice(key),
                        value: entry.value().clone(),
                        expires_at: entry.expires_at(),
                    });
                }
            }
        }
        Ok(snapshot)
    }
}

fn remove_if_expired(shard: &Shard, key: &[u8], now: SystemTime) -> Result<bool, StoreError> {
    let mut entries = shard.write()?;
    if entries
        .get(key)
        .is_some_and(|entry| entry.is_expired_at(now))
    {
        entries.remove(key);
        Ok(true)
    } else {
        Ok(false)
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
