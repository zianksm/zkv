use std::path::PathBuf;

use crossbeam_channel::{ Receiver, Sender };
use uuid::Uuid;

use crate::primitives::Bucket;

pub enum FsMessage {
    Stop,
    Load(PathBuf),
    Result(Bucket),
}

pub struct Fs {
    task_handle: Receiver<FsMessage>,
    out: Sender<FsMessage>,
}

impl Fs {
    // will return a sender for sending data, and a receiver for receiving data
    pub fn new() -> (Self, Sender<FsMessage>, Receiver<FsMessage>) {
        // for receiving tasks
        let (task_command_buffer, task_buffer) = crossbeam_channel::unbounded();
        // for sending result
        let (out_buffer, out_command_buffer) = crossbeam_channel::unbounded();

        (Fs { task_handle: task_buffer, out: out_buffer }, task_command_buffer, out_command_buffer)
    }

    pub fn run(mut self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || self.run_in_thread())
    }

    fn run_in_thread(mut self) {
        loop {
            match self.task_handle.recv() {
                Ok(FsMessage::Stop) => {
                    break;
                }
                Ok(FsMessage::Load(path)) => {
                    let result = self.load(path);
                    self.out.send(FsMessage::Result(result));
                }
                _ => {}
            }
        }
    }

    fn load(&self, path: PathBuf) -> Bucket {
        Bucket::from_file(path).unwrap_or_else(|_| Bucket::new(Some(Uuid::now_v7().to_string())))
    }
}
