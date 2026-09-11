//! Stage 1: A simple in-memory key-value store.
//!
//! Goal: get comfortable with the basic data structure before adding
//! persistence (Stage 2) or distribution (Stage 3+).
//!
//! TODO:
//! - [ ] Implement get/set/delete backed by a HashMap
//! - [ ] Add a `keys()` / `scan(prefix)` method
//! - [ ] Think about what happens on concurrent access (Mutex? RwLock?)

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

    pub fn get(&self, _key: &str) -> Option<&String> {
        todo!("look up the key in self.data")
    }

    pub fn set(&mut self, _key: String, _value: String) {
        todo!("insert into self.data")
    }

    pub fn delete(&mut self, _key: &str) -> Option<String> {
        todo!("remove from self.data")
    }
}
