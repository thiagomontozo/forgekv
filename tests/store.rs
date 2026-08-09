use std::{sync::Arc, thread, time::SystemTime};

use bytes::Bytes;
use forgekv::{
    metrics::Metrics,
    store::{ShardedStore, TtlState},
};

fn store() -> ShardedStore {
    ShardedStore::new(16, Arc::new(Metrics::default())).expect("valid shard count")
}

#[test]
fn set_get_overwrite_delete_and_exists() {
    let store = store();
    store
        .set(Bytes::from_static(b"key"), Bytes::from_static(b"one"))
        .expect("set should work");
    assert_eq!(
        store.get(b"key").expect("get should work"),
        Some(Bytes::from_static(b"one"))
    );
    store
        .set(Bytes::from_static(b"key"), Bytes::from_static(b"two"))
        .expect("overwrite should work");
    assert!(store.exists(b"key").expect("exists should work"));
    assert!(store.delete(b"key").expect("delete should work"));
    assert!(!store.exists(b"key").expect("exists should work"));
}

#[test]
fn lazy_expiration_and_persist_are_deterministic() {
    let store = store();
    store
        .set_with_expiry(
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"value"),
            Some(SystemTime::UNIX_EPOCH),
        )
        .expect("set should work");
    assert_eq!(store.get(b"expired").expect("get should work"), None);

    let future = SystemTime::now()
        .checked_add(std::time::Duration::from_secs(60))
        .expect("test expiration should fit");
    store
        .set_with_expiry(
            Bytes::from_static(b"live"),
            Bytes::from_static(b"value"),
            Some(future),
        )
        .expect("set should work");
    assert!(matches!(
        store.ttl(b"live").expect("ttl should work"),
        TtlState::ExpiresIn(_)
    ));
    assert!(store.persist(b"live").expect("persist should work"));
    assert_eq!(
        store.ttl(b"live").expect("ttl should work"),
        TtlState::Persistent
    );
}

#[test]
fn shard_selection_is_stable() {
    let store = store();
    let second_store = store();
    assert_eq!(
        store.shard_index(b"same-key"),
        second_store.shard_index(b"same-key")
    );
    assert!(store.shard_index(b"key") < store.shard_count());
}

#[test]
fn concurrent_access_uses_independent_keys() {
    let store = Arc::new(store());
    let handles: Vec<_> = (0..8)
        .map(|worker| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for sequence in 0..250 {
                    let key = Bytes::from(format!("worker:{worker}:{sequence}"));
                    store
                        .set(key.clone(), Bytes::from_static(b"value"))
                        .expect("set should work");
                    assert!(store.exists(&key).expect("exists should work"));
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("worker should not panic");
    }
    assert_eq!(store.len().expect("len should work"), 2_000);
}
