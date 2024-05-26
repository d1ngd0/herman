mod ledger;
mod memberlist;

use std::{future::Future, sync::Arc};

use ::memberlist::{
    delegate::Delegate,
    net::{AddressResolver, Transport},
    transport, Memberlist,
};
use ledger::Ledger;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{broadcast, mpsc, Mutex};

use self::{
    ledger::{Data, Entry, Key},
    memberlist::{Broadcaster, Subscriber},
};

pub use self::memberlist::{Broadcast, HermanDelegate, Subscribe};

/// Broadcase is used to send data to multiple nodes on the network. It is up to
/// the implementor how this should be done. send_entry is used to send a single entry
/// whenever we encounter new data, or at regular intervals all entries within the ledger
/// are sent to ensure all nodes are in sync with one another.
pub trait Broadcast<T: Key, D: Data> {
    /// send_entry is used to send an entry to one or more nodes on the network.
    fn send_entry(&mut self, entry: &Entry<T, D>) -> impl Future<Output = ()>;
    // TODO: We will likely need an error from this
}

/// Subscribe is used to watch for incoming changes. It is the partner of the Broadcast
/// trait. The subscriber implements a future trait that is ued to grab an entry
/// from the broadcast of another node.
pub trait Subscribe<T: Key, D: Data> {
    fn watch(&mut self) -> impl Future<Output = Entry<T, D>>;
}

/// Config holds the config data, and handles the broadcast and subscribers for the
/// ledger.
pub struct Config<T: Key, D: Data, B: Broadcast<T, D>> {
    ledger: Arc<Mutex<Ledger<T, D>>>,
    broadcast: Arc<Mutex<B>>,
    drop: Arc<Mutex<bool>>,
}

impl<
        T: Key + Serialize + DeserializeOwned,
        D: Data + Serialize + DeserializeOwned,
        Tr: Transport,
    > Config<T, D, Broadcaster<Tr>>
{
    pub async fn new_with_memberlist(
        transport_options: Tr::Options,
        opts: ::memberlist::Options,
    ) -> Result<
        Config<T, D, Broadcaster<Tr>>,
        ::memberlist::error::Error<
            Tr,
            HermanDelegate<Tr::Id, <Tr::Resolver as AddressResolver>::ResolvedAddress>,
        >,
    > {
        // TODO: we need to make the channel size and the number of friends configurable
        let (tx, rx) = mpsc::channel(100);
        let delegate = HermanDelegate::with_messages(tx);
        let subscriber = Subscriber::new(rx);
        let memberlist = Memberlist::with_delegate(delegate, transport_options, opts).await?;
        let broadcast = Broadcaster::new(memberlist, 3);

        // TODO start the threads

        Ok(Self {
            ledger: Arc::new(Mutex::new(Ledger::new())),
            broadcast: Arc::new(Mutex::new(broadcast)),
            drop: Arc::new(Mutex::new(false)),
        })
    }
}

impl<T: Key, D: Data, B: Broadcast<T, D>> Config<T, D, B> {
    /// new creates a new Config with the given ledger, broadcast, and subscriber.
    pub fn new(broadcast: B) -> Self {
        Self {
            ledger: Arc::new(Mutex::new(Ledger::new())),
            broadcast: Arc::new(Mutex::new(broadcast)),
            drop: Arc::new(Mutex::new(false)),
        }
    }

    pub fn broadcast_listener<S: Subscribe<T, D>>(
        &self,
        subscriber: S,
    ) -> BroadcastListener<T, D, B, S> {
        BroadcastListener {
            ledger: self.ledger.clone(),
            broadcast: self.broadcast.clone(),
            drop: self.drop.clone(),
            subscriber,
        }
    }

    pub async fn put(&mut self, key: T, value: D) {
        let mut ledger = self.ledger.lock().await;
        let entry = ledger.put(key, value);
        self.broadcast.lock().await.send_entry(entry).await;
        ledger.sort();
    }

    pub async fn delete(&mut self, key: T) {
        let mut ledger = self.ledger.lock().await;
        let entry = ledger.delete(key);
        self.broadcast.lock().await.send_entry(entry).await;
        ledger.sort();
    }

    pub async fn get<ET: PartialEq<T>>(&mut self, key: ET) -> Option<D> {
        let ledger = self.ledger.lock().await;
        ledger.get(key).cloned()
    }
}

impl<T: Key, D: Data, B: Broadcast<T, D>> Drop for Config<T, D, B> {
    fn drop(&mut self) {
        let drop = self.drop.clone();
        tokio::spawn(async move {
            let mut drop = drop.lock().await;
            *drop = true;
        });
    }
}

pub struct BroadcastListener<T: Key, D: Data, B: Broadcast<T, D>, S: Subscribe<T, D>> {
    ledger: Arc<Mutex<Ledger<T, D>>>,
    broadcast: Arc<Mutex<B>>,
    drop: Arc<Mutex<bool>>,
    subscriber: S,
}

// TODO: We need to add a way to stop the listener
impl<T: Key, D: Data, B: Broadcast<T, D>, S: Subscribe<T, D>> BroadcastListener<T, D, B, S> {
    pub async fn run(&mut self) {
        loop {
            let entry = self.subscriber.watch().await;
            let mut ledger = self.ledger.lock().await;
            if ledger.add_entry(entry.clone()) {
                self.broadcast.lock().await.send_entry(&entry).await;
            }
            ledger.sort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{sync::broadcast, time::sleep};

    struct TestBroadcast<T: Key, D: Data> {
        tx: broadcast::Sender<Entry<T, D>>,
    }

    impl<T: Key, D: Data> Broadcast<T, D> for TestBroadcast<T, D> {
        async fn send_entry(&mut self, entry: &Entry<T, D>) {
            let _ = self.tx.send(entry.clone());
        }
    }

    struct TestSubscriber<T: Key, D: Data> {
        rx: broadcast::Receiver<Entry<T, D>>,
    }

    impl<T: Key, D: Data> Subscribe<T, D> for TestSubscriber<T, D> {
        async fn watch(&mut self) -> Entry<T, D> {
            self.rx.recv().await.unwrap()
        }
    }

    #[tokio::test]
    async fn test_config() {
        // set up some local broadcasting stuff
        let (tx, rx) = broadcast::channel(10);
        let broadcast = TestBroadcast { tx: tx.clone() };
        let subscriber = TestSubscriber { rx };
        let mut config = Config::new(broadcast);

        let mut dup_config = Config::new(TestBroadcast { tx: tx.clone() });
        let mut runner = dup_config.broadcast_listener(subscriber);
        tokio::spawn(async move {
            runner.run().await;
        });

        // create some entries in config
        config
            .put("something".to_string(), "value".to_string())
            .await;
        config
            .put("another_thing".to_string(), "value2".to_string())
            .await;

        // wait for convergence
        sleep(std::time::Duration::from_secs(1)).await;

        // Check that they are now in the duplicate config
        assert_eq!(dup_config.get("something").await, Some("value".to_string()));
        assert_eq!(
            dup_config.get("another_thing").await,
            Some("value2".to_string())
        );
        assert_eq!(dup_config.get("nothing").await, None);
    }
}
