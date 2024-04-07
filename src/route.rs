use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Receiver, SyncSender},
        RwLock,
    },
};

use crate::Dapt;

// Router is a collection of namespaces which allows for registering
// channels to read and write to namespace. Namespaces are created
// as needed in the router during registration. If the all input and
// output registers have been closed the namespace will automatically
// be cleaned up.
pub struct Router {
    namespaces: HashMap<String, Namespace>,
}

// Namespace is a location to place dapt packets. When registering to a
// namespace to write a channel is created to send dapt packets. When
// reading data from a namespace, a channel is provided to read dapt
// packets. Each output registered will recieve all of the data in the
// namespace. The namespace itself does not buffer any data, though buffers
// can be set on the registers themselves.
pub struct Namespace {
    inputs: RwLock<HashMap<String, Receiver<Dapt>>>,
    outputs: RwLock<HashMap<String, SyncSender<Dapt>>>,
}

impl Namespace {
    pub fn new() -> Self {
        Namespace {
            inputs: RwLock::new(HashMap::new()),
            outputs: RwLock::new(HashMap::new()),
        }
    }

    pub fn input(&mut self, name: String, buffer: usize) -> SyncSender<Dapt> {
        let (send, recv) = mpsc::sync_channel(buffer);
        self.inputs.write().unwrap().insert(name, recv);
        send
    }

    pub fn output(&mut self, name: String, buffer: usize) -> Receiver<Dapt> {
        let (send, recv) = mpsc::sync_channel(buffer);
        self.outputs.write().unwrap().insert(name, send);
        recv
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Namespace::new()
    }
}
