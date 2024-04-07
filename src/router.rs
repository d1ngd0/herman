use std::collections::HashMap;

use dapt::Dapt;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    RwLock,
};

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
    outputs: RwLock<HashMap<String, Sender<Dapt>>>,
}

impl Namespace {
    pub fn new() -> Self {
        Namespace {
            inputs: RwLock::new(HashMap::new()),
            outputs: RwLock::new(HashMap::new()),
        }
    }

    pub async fn input(&mut self, name: String, buffer: usize) -> Sender<Dapt> {
        let (send, recv) = channel(buffer);
        self.inputs.write().await.insert(name, recv);
        send
    }

    pub async fn output(&mut self, name: String, buffer: usize) -> Receiver<Dapt> {
        let (send, recv) = channel(buffer);
        self.outputs.write().await.insert(name, send);
        recv
    }
}

impl Drop for Namespace {
    fn drop(&mut self) {
        // Clean up the namespace
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Namespace::new()
    }
}
