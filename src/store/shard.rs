use std::{
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::error::StoreError;

use super::Entry;

#[derive(Debug, Default)]
pub(super) struct Shard {
    entries: RwLock<HashMap<Vec<u8>, Entry>>,
}

impl Shard {
    pub(super) fn read(&self) -> Result<RwLockReadGuard<'_, HashMap<Vec<u8>, Entry>>, StoreError> {
        self.entries.read().map_err(|_| StoreError::LockPoisoned)
    }

    pub(super) fn write(
        &self,
    ) -> Result<RwLockWriteGuard<'_, HashMap<Vec<u8>, Entry>>, StoreError> {
        self.entries.write().map_err(|_| StoreError::LockPoisoned)
    }
}
