use tokio::sync::mpsc::Sender;

use super::ledger::{Data, Entry, EventListener, Key};

pub struct StateEngine {}

pub struct StateEngineEventListener<K: Key, D: Data> {
    sender: Sender<Entry<K, D>>,
}

impl<K: Key, D: Data> EventListener<K, D> for StateEngineEventListener<K, D> {
    fn on_event(&mut self, event: &Entry<K, D>) {
        match self.sender.blocking_send(event.clone()) {
            Ok(_) => {}
            Err(e) => tracing::error!("could not send entry: {}", e),
        }
    }
}
