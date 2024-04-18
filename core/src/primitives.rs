use std::{ fmt::Debug, str::FromStr };
use std::collections::HashMap;

use serde::{ Deserialize, Serialize };

pub const KEY_LENGTH: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(u128);

impl FromStr for Key {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hash = xxhash_rust::xxh3::xxh3_128(s.as_bytes());
        Ok(Key(hash))
    }
}

pub trait Storage {
    fn get(&self, key: &Key) -> Option<String>;

    fn set(&mut self, key: Key, value: &str);

    fn id(&self) -> String;

    fn dump(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partition {
    storage: HashMap<Key, String, twox_hash::RandomXxHashBuilder64>,
    id: String,
}

impl Storage for Partition {
    fn get(&self, key: &Key) -> Option<String> {
        self.storage.get(key).cloned()
    }

    fn set(&mut self, key: Key, value: &str) {
        self.storage.insert(key, value.to_string());
    }

    fn id(&self) -> String {
        todo!()
    }

    fn dump(&self) -> String {
        todo!()
    }
}
