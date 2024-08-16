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

pub trait Storage {
    fn get(&self, key: &Key) -> Option<String>;

    fn set(&mut self, key: Key, value: String) -> Option<String>;

    fn id(&self) -> &Uuid;

    /// Dump the storage to a JSON formatted value
    fn dump(&self) -> SerializedPartition;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionStorage {
    storage: HashMap<Key, String, twox_hash::RandomXxHashBuilder64>,
    id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartition {
    id: Uuid,
    storage: serde_json::Value,
}

impl SerializedPartition {
    pub fn try_from_value(value: serde_json::Value) -> Option<Self> {
        let id = value.get("id")?.as_str()?.parse().ok()?;

        Some(Self {
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

impl Default for PartitionStorage {
    fn default() -> Self {
        Self::new()
    }
}

pub enum PartitionInitError {
    /// The file could not be opened, or invalid
    FileError,
    /// The file could not be parsed
    ParseError,
}

impl PartitionStorage {
    pub fn new() -> Self {
        Self {
            storage: Default::default(),
            id: uuid::Uuid::now_v7(),
        }
    }

    pub fn from_file(path: PathBuf) -> Result<Self, PartitionInitError> {
        let contents = std::fs::read_to_string(&path).map_err(|_| PartitionInitError::FileError)?;

        let serialized: SerializedPartition = serde_json
            ::from_str(&contents)
            .map_err(|_| PartitionInitError::ParseError)?;

        Ok(Self {
            storage: serde_json
                ::from_value(serialized.storage)
                .expect("HashMap should deserialize. this is a bug"),
            id: serialized.id,
        })
    }
}

impl Storage for PartitionStorage {
    fn get(&self, key: &Key) -> Option<String> {
        self.storage.get(key).cloned()
    }

    fn set(&mut self, key: Key, value: String) -> Option<String> {
        self.storage.insert(key, value)
    }

    fn id(&self) -> &Uuid {
        &self.id
    }

    fn dump(&self) -> SerializedPartition {
        let storage = serde_json
            ::to_value(&self.storage)
            .expect("HashMap should serialize. this is a bug");

        SerializedPartition {
            id: self.id,
            storage,
        }
    }
}
