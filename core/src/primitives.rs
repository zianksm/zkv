use std::path::PathBuf;
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::sync::Arc;
use std::thread;
use std::{ fmt::Debug, str::FromStr };
use std::collections::HashMap;

use crossbeam_channel::{ select, Receiver, Sender };
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

    fn set(&mut self, key: Key, value: &str);

    fn id(&self) -> Uuid;

    fn dump(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionStorage {
    storage: HashMap<Key, String, twox_hash::RandomXxHashBuilder64>,
    id: Uuid,
}

impl PartitionStorage {
    pub fn new() -> Self {
        Self {
            storage: Default::default(),
            id: uuid::Uuid::new_v4(),
        }
    }

    pub fn init(path: PathBuf) -> Self {
        todo!()
    }
}

impl Storage for PartitionStorage {
    fn get(&self, key: &Key) -> Option<String> {
        self.storage.get(key).cloned()
    }

    fn set(&mut self, key: Key, value: &str) {
        self.storage.insert(key, value.to_string());
    }

    fn id(&self) -> Uuid {
        todo!()
    }

    fn dump(&self) -> String {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stat {
    // must be none on sending, and some on receiving
    Ops(Option<u16>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Task {
    Get(Key),
    Set(Key, String),
    Dump,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskResult {
    Get(Option<String>),
    Set,
    Dump(String),
}

#[derive(Debug, Clone)]
pub struct Channels<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> Default for Channels<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Channels<T> {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        Self { sender, receiver }
    }
}

impl<T> AsRef<Sender<T>> for Channels<T> {
    fn as_ref(&self) -> &Sender<T> {
        &self.sender
    }
}

impl<T> AsRef<Receiver<T>> for Channels<T> {
    fn as_ref(&self) -> &Receiver<T> {
        &self.receiver
    }
}

pub trait MultiSend<T> {
    fn send(&self, msg: T);
}

impl MultiSend<Task> for Vec<PartitionChannels> {
    fn send(&self, msg: Task) {
        // panic if the task is not a read ops
        match msg {
            Task::Get(_) => {
                for channel in self {
                    channel.send_task(msg.clone());
                }
            }
            _ => panic!("Only read ops are allowed to be broadcasted"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartitionChannels {
    task_channel: Channels<Task>,
    stat_channel: Channels<Stat>,
}

impl PartitionChannels {
    fn send_task(&self, task: Task) {
        <Channels<Task> as AsRef<Sender<Task>>>::as_ref(&self.task_channel).send(task).unwrap()
    }

    fn send_stat(&self, stat: Stat) {
        <Channels<Stat> as AsRef<Sender<Stat>>>::as_ref(&self.stat_channel).send(stat).unwrap()
    }
}

pub struct Partition {
    controller_channel: Sender<TaskResult>,
    channels: PartitionChannels,
    storage: PartitionStorage,
    ops_count: Arc<AtomicUsize>,
}

pub struct PartitionHandle {
    channels: PartitionChannels,
    ops_count: Arc<AtomicUsize>,
    thread_hanle: thread::JoinHandle<()>,
}

impl Partition {
    pub fn new(controller_channel: Sender<TaskResult>) -> Self {
        Self {
            controller_channel,
            channels: Default::default(),
            storage: PartitionStorage::new(),
            ops_count: Arc::new(AtomicUsize::new(0)),
        }
    }
    
    pub fn increment_counter(&self) {
        self.ops_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_counter(&self) -> usize {
        self.ops_count.load(Ordering::Relaxed)
    }

    pub fn reset_counter(&self) {
        self.ops_count.store(0, Ordering::Relaxed);
    }

    pub fn run(self) -> PartitionHandle {
        let mut storage = self.storage;
        let channels = self.channels;
        let ops_count = self.ops_count;

        let handle = Self::run_in_threads(channels.clone(), storage, ops_count.clone());

        PartitionHandle {
            channels,
            ops_count,
            thread_hanle: handle,
        }
    }

    fn run_in_threads(
        channels: PartitionChannels,
        mut storage: PartitionStorage,
        ops_count: Arc<AtomicUsize>
    ) -> thread::JoinHandle<()> {
        Self::monitor_ops(channels.clone(), ops_count);

        thread::Builder
            ::new()
            .name("partition".to_string())
            .spawn(move || {
                loop {
                    match
                        <Channels<Task> as AsRef<Receiver<Task>>>
                            ::as_ref(&channels.task_channel)
                            .recv()
                    {
                        Ok(Task::Get(key)) => {
                            // let _ = channels.stat_channel.as_ref().send(Stat::Ops(Some(1)));
                            // let _ = channels.stat_channel.as_ref().send(Stat::Ops(None));
                        }
                        Ok(Task::Set(key, value)) => {
                            storage.set(key, &value);
                            // let _ = channels.stat_channel.as_ref().send(Stat::Ops(Some(1)));
                            // let _ = channels.stat_channel.as_ref().send(Stat::Ops(None));
                        }
                        Ok(Task::Dump) => {
                            // let _ = channels.stat_channel.as_ref.send(Stat::Ops(Some(1)));
                            // let _ = channels.stat_channel.as_ref().send(Stat::Ops(None));
                        }
                        Ok(Task::Stop) => {
                            break;
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
            })
            .unwrap()
    }

    fn monitor_ops(channels: PartitionChannels, ops_count: Arc<AtomicUsize>) {
        thread::Builder
            ::new()
            .name("count resetter".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(std::time::Duration::from_secs(1));
                    ops_count.store(0, Ordering::Relaxed);
                }
            });
    }
}
