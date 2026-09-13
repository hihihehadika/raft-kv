//! Stage 1: A simple in-memory key-value store.
//!
//! This is the state machine that Raft will drive in later stages.
//! Every committed Raft log entry will eventually call `set` or `delete`
//! on this struct — so keeping it simple and correct here matters.

use std::collections::HashMap;

pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            data: HashMap::new(),
        }
    }

    pub fn dump(&self) -> HashMap<String, String> {
        self.data.clone()
    }

    pub fn load(&mut self, data: HashMap<String, String>) {
        self.data = data;
    }

    /// Returns a reference to the value for `key`, or `None` if not found.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    /// Inserts or overwrites `key` with `value`.
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    /// Removes `key` and returns its previous value, or `None` if not found.
    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.data.remove(key)
    }

    /// Returns all keys in the store (order is arbitrary — HashMap).
    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    /// Returns all (key, value) pairs whose key starts with `prefix`,
    /// sorted alphabetically by key.
    pub fn scan(&self, prefix: &str) -> Vec<(&String, &String)> {
        let mut results: Vec<(&String, &String)> = self
            .data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect();
        results.sort_by_key(|(k, _)| *k);
        results
    }

    /// Returns the total number of keys currently held in the store.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the store has no entries.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// --------------------------------------------------------------------------
// Unit tests
// --------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> KvStore {
        let mut kv = KvStore::new();
        kv.set("user:1".into(), "alice".into());
        kv.set("user:2".into(), "bob".into());
        kv.set("user:3".into(), "carol".into());
        kv.set("config:host".into(), "localhost".into());
        kv.set("config:port".into(), "8080".into());
        kv
    }

    #[test]
    fn test_new_store_is_empty() {
        let kv = KvStore::new();
        assert!(kv.is_empty());
        assert_eq!(kv.len(), 0);
    }

    #[test]
    fn test_set_and_get() {
        let mut kv = KvStore::new();
        kv.set("foo".into(), "bar".into());
        assert_eq!(kv.get("foo"), Some(&"bar".to_string()));
    }

    #[test]
    fn test_get_missing_key_returns_none() {
        let kv = KvStore::new();
        assert_eq!(kv.get("ghost"), None);
    }

    #[test]
    fn test_overwrite_existing_key() {
        let mut kv = KvStore::new();
        kv.set("k".into(), "v1".into());
        kv.set("k".into(), "v2".into());
        assert_eq!(kv.get("k"), Some(&"v2".to_string()));
        assert_eq!(kv.len(), 1); // still one entry
    }

    #[test]
    fn test_delete_existing_key() {
        let mut kv = KvStore::new();
        kv.set("x".into(), "42".into());
        let old = kv.delete("x");
        assert_eq!(old, Some("42".to_string()));
        assert_eq!(kv.get("x"), None);
    }

    #[test]
    fn test_delete_missing_key_returns_none() {
        let mut kv = KvStore::new();
        assert_eq!(kv.delete("nope"), None);
    }

    #[test]
    fn test_len_and_is_empty() {
        let kv = make_store();
        assert!(!kv.is_empty());
        assert_eq!(kv.len(), 5);
    }

    #[test]
    fn test_keys_returns_all_keys() {
        let kv = make_store();
        let mut keys: Vec<&String> = kv.keys();
        keys.sort();
        assert_eq!(
            keys,
            vec!["config:host", "config:port", "user:1", "user:2", "user:3"]
        );
    }

    #[test]
    fn test_scan_prefix() {
        let kv = make_store();
        let results = kv.scan("user:");
        // scan returns sorted by key
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (&"user:1".to_string(), &"alice".to_string()));
        assert_eq!(results[1], (&"user:2".to_string(), &"bob".to_string()));
        assert_eq!(results[2], (&"user:3".to_string(), &"carol".to_string()));
    }

    #[test]
    fn test_scan_no_match() {
        let kv = make_store();
        let results = kv.scan("nope:");
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_empty_prefix_returns_all_sorted() {
        let kv = make_store();
        let results = kv.scan("");
        assert_eq!(results.len(), 5); // everything matches empty prefix
    }
}
