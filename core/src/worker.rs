use crossbeam_channel::{ Receiver, Sender };

use crate::primitives::{ Key, Bucket, SerializedBucket, Storage };

#[derive(Debug, Clone)]
pub struct ReadMessage {
    pub key: Key,
    pub req_id: uuid::Uuid,
}

impl ReadMessage {
    pub fn new(key: Key) -> Self {
        Self { key, req_id: uuid::Uuid::now_v7() }
    }
}
#[derive(Debug, Clone)]
pub struct WriteMessage {
    pub key: Key,
    pub req_id: uuid::Uuid,
    pub value: crate::primitives::Value,
}

impl WriteMessage {
    pub fn new(key: Key, value: crate::primitives::Value) -> Self {
        Self { key, req_id: uuid::Uuid::now_v7(), value }
    }
}

#[derive(Debug, Clone)]
pub struct OpResult {
    pub result: Option<crate::primitives::Value>,
    pub req_id: uuid::Uuid,
}

impl OpResult {
    pub fn new(result: Option<crate::primitives::Value>, req_id: uuid::Uuid) -> Self {
        Self { result, req_id }
    }
}

#[derive(Debug, Clone)]
pub enum WorkerMessage {
    Stop,
    Read(ReadMessage),
    Write(WriteMessage),
    Dump,
    TaskResult(TaskResult),
}

#[derive(Debug, Clone)]
pub enum TaskResult {
    StopFlag,
    ReadResult(OpResult),
    WriteResult(OpResult),
    DumpResult(SerializedBucket),
}

pub struct BucketWorker {
    storage: Bucket,
    task_handle: Receiver<WorkerMessage>,
    out: Sender<WorkerMessage>,
}

impl BucketWorker {
    pub fn new(
        storage: Bucket,
        task_handle: Receiver<WorkerMessage>
    ) -> (Self, Receiver<WorkerMessage>) {
        let (out, out_handle) = crossbeam_channel::unbounded();

        (BucketWorker { storage, task_handle, out }, out_handle)
    }

    pub fn run(mut self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || Self::_run_in_thread(self))
    }

    fn _run_in_thread(mut self) {
        loop {
            match self.task_handle.recv() {
                Ok(WorkerMessage::Stop) => {
                    self.out.send(WorkerMessage::TaskResult(TaskResult::StopFlag));
                    break;
                }
                Ok(WorkerMessage::Read(msg)) => {
                    let result = self.storage.get(&msg.key);
                    let result = OpResult::new(result, msg.req_id);
                    self.out.send(WorkerMessage::TaskResult(TaskResult::ReadResult(result)));
                }
                Ok(WorkerMessage::Write(msg)) => {
                    let result = self.storage.set(msg.key, msg.value);
                    let result = OpResult::new(result, msg.req_id);
                    self.out.send(WorkerMessage::TaskResult(TaskResult::WriteResult(result)));
                }
                Ok(WorkerMessage::Dump) => {
                    let result = self.storage.dump();
                    self.out.send(WorkerMessage::TaskResult(TaskResult::DumpResult(result)));
                }
                Err(e) => {
                    log::error!(
                        "PartitionWorker: task_handle channel received invalid command : {:?}",
                        e
                    );
                }
                Ok(WorkerMessage::TaskResult(_)) => {
                    log::error!("PartitionWorker: task_handle channel received TaskResult");
                }
            }
        }
    }
}
