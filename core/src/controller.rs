use std::path::PathBuf;

use crate::primitives::{ Channels, Key, Partition, PartitionHandle, Task, TaskResult };

pub struct Controller {
    partitions: Vec<PartitionHandle>,
    channels: Channels<Task, TaskResult>,
}

impl Controller {
    pub fn init_from_files(files: Vec<PathBuf>) {
        todo!()
    }

    pub fn new() {
        let channels = Channels::<Task,TaskResult>::new();
        let initial_partition = Partition::new(channels.);
        Self {
            partitions: vec![],
            channels: todo!(),
        }
    }

    pub fn create_new_partition_with_name(name: String) {
        todo!()
    }

    pub fn read_from_partition(key: Key, partition_name: String) {
        todo!()
    }

    pub fn write_to_partition(key: Key, value: String, partition_name: String) {
        todo!()
    }
}
