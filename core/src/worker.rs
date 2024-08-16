use crossbeam_channel::{ Receiver, Sender };

use crate::primitives::{ Key, PartitionStorage, SerializedPartition, Storage };

#[derive(Debug, Clone)]
pub enum Message {
    Stop,
    Read(Key),
    Write(Key, String),
    Dump,
    TaskResult(TaskResult),
}

#[derive(Debug, Clone)]
pub enum TaskResult {
    StopFlag,
    ReadResult(Option<String>),
    WriteResult(Option<String>),
    DumpResult(SerializedPartition),
}

pub struct PartitionWorker {
    storage: PartitionStorage,
    task_handle: Receiver<Message>,
    out: Sender<Message>,
}

impl PartitionWorker {
    pub fn new(
        storage: PartitionStorage,
        task_handle: Receiver<Message>
    ) -> (Self, Receiver<Message>) {
        let (out, out_handle) = crossbeam_channel::unbounded();

        (PartitionWorker { storage, task_handle, out }, out_handle)
    }

    pub fn run(mut self) {
        std::thread::spawn(move ||Self::_run_in_thread(self));
    }

    fn _run_in_thread(mut self) {
        loop {
            match self.task_handle.recv() {
                Ok(Message::Stop) => {
                    self.out.send(Message::TaskResult(TaskResult::StopFlag));
                    break;
                }
                Ok(Message::Read(key)) => {
                    let result = self.storage.get(&key);
                    self.out.send(Message::TaskResult(TaskResult::ReadResult(result)));
                }
                Ok(Message::Write(key, value)) => {
                    let result = self.storage.set(key, value);
                    self.out.send(Message::TaskResult(TaskResult::WriteResult(result)));
                }
                Ok(Message::Dump) => {
                    let result = self.storage.dump();
                    self.out.send(Message::TaskResult(TaskResult::DumpResult(result)));
                }
                Err(e) => {
                    log::error!(
                        "PartitionWorker: task_handle channel received invalid command : {:?}",
                        e
                    );
                }
                Ok(Message::TaskResult(_)) => {
                    log::error!("PartitionWorker: task_handle channel received TaskResult");
                }
            }
        }
    }
}
