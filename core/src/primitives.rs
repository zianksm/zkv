use std::path::PathBuf;
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::sync::Arc;
use std::thread;
use std::{ fmt::Debug, str::FromStr };
use std::collections::HashMap;

use crossbeam_channel::{ Receiver, Sender };
use serde::{ Deserialize, Serialize };
use uuid::Uuid;

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

pub type Value = Vec<u8>;


pub trait Storage {
    fn label(&self) -> &str;

    fn get(&self, key: &Key) -> Option<Value>;

    fn set(&mut self, key: Key, value: Value) -> Option<Value>;

    fn id(&self) -> &Uuid;

    /// Dump the storage to a JSON formatted value
    fn dump(&self) -> SerializedBucket;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bucket {
    label: String,
    storage: HashMap<Key, Value, twox_hash::RandomXxHashBuilder64>,
    id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedBucket {
    label: String,
    id: Uuid,
    storage: serde_json::Value,
}

impl SerializedBucket {
    pub fn try_from_value(value: serde_json::Value) -> Option<Self> {
        let id = value.get("id")?.as_str()?.parse().ok()?;
        let label = value.get("label")?.as_str()?.to_string();

        Some(Self {
            label,
            id,
            storage: value.get("storage")?.clone(),
        })
    }

    pub fn write_to(&self, path: PathBuf) -> Result<(), std::io::Error> {
        let serialized = serde_json
            ::to_string_pretty(self)
            .expect("SerializedPartition should serialize. this is a bug");

        std::fs::write(path, serialized)
    }
}

impl Default for Bucket {
    fn default() -> Self {
        Self::new(None)
    }
}

pub enum PartitionInitError {
    /// The file could not be opened, or invalid
    FileError,
    /// The file could not be parsed
    ParseError,
}

impl Bucket {
    pub fn new(label: Option<String>) -> Self {
        Self {
            label: label.unwrap_or_else(|| "default".to_string()),
            storage: Default::default(),
            id: uuid::Uuid::now_v7(),
        }
    }

    pub fn from_file(path: PathBuf) -> Result<Self, PartitionInitError> {
        let contents = std::fs::read_to_string(&path).map_err(|_| PartitionInitError::FileError)?;

        let serialized: SerializedBucket = serde_json
            ::from_str(&contents)
            .map_err(|_| PartitionInitError::ParseError)?;

        Ok(Self {
            label: serialized.label,
            storage: serde_json
                ::from_value(serialized.storage)
                .expect("HashMap should deserialize. this is a bug"),
            id: serialized.id,
        })
    }
}

impl Storage for Bucket {
    fn get(&self, key: &Key) -> Option<Value> {
        self.storage.get(key).cloned()
    }

    fn set(&mut self, key: Key, value: Value) -> Option<Value> {
        self.storage.insert(key, value)
    }

    fn id(&self) -> &Uuid {
        &self.id
    }

    fn dump(&self) -> SerializedBucket {
        let storage = serde_json
            ::to_value(&self.storage)
            .expect("HashMap should serialize. this is a bug");

        SerializedBucket {
            label: self.label.clone(),
            id: self.id,
            storage,
        }
    }

    fn label(&self) -> &str {
        &self.label
    }
}
