use std::{ collections::HashMap, thread };

use crossbeam_channel::{ Receiver, Sender };

use crate::{ fs::FsMessage, worker::WorkerMessage };

pub struct Ref<Message, ThreadHandle = ()> {
    thread_handle: thread::JoinHandle<ThreadHandle>,
    out_buf: Receiver<Message>,
    input_buf: Sender<Message>,
}

pub struct Controller {
    workers: HashMap<String, Ref<WorkerMessage>>,
    fs: Ref<FsMessage>,
}

impl Controller {
    pub fn new() -> Self {
        let (fs, fs_task_input, fs_task_output) = crate::fs::Fs::new();
        let fs = Ref { thread_handle: fs.run(), out_buf: fs_task_output, input_buf: fs_task_input };

        Controller { workers: HashMap::new(), fs }
    }
}
