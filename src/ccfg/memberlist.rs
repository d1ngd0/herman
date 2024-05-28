use std::{
    future::Future,
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
    sync::Arc,
};

use memberlist::{
    bytes::Bytes,
    delegate::{
        AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, NodeDelegate,
        PingDelegate, VoidDelegateError,
    },
    net::{AddressResolver, Id, Transport},
    types::{Meta, NodeState, SmallVec, TinyVec},
    CheapClone, Memberlist,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::Config;

use super::{
    ledger::{Data, Entry, Key},
    Broadcast, Subscribe,
};

pub trait WithHermanDelegate<
    T: Transport,
    K: Key + DeserializeOwned + Serialize,
    D: Data + DeserializeOwned + Serialize,
>
{
    fn with_config(
        transport_options: T::Options,
        opts: memberlist::Options,
    ) -> impl Future<
        Output = Result<
            (
                Arc<
                    Memberlist<
                        T,
                        HermanDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
                    >,
                >,
                Config<K, D, Broadcaster<T>>,
            ),
            memberlist::error::Error<
                T,
                HermanDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
            >,
        >,
    >;
}

impl<
        T: Transport,
        K: Key + DeserializeOwned + Serialize,
        D: Data + DeserializeOwned + Serialize,
    > WithHermanDelegate<T, K, D>
    for Memberlist<T, HermanDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>
{
    async fn with_config(
        transport_options: T::Options,
        opts: memberlist::Options,
    ) -> Result<
        (Arc<Self>, Config<K, D, Broadcaster<T>>),
        memberlist::error::Error<
            T,
            HermanDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
        >,
    > {
        let (tx, rx) = mpsc::channel(100);
        let delegate = HermanDelegate::with_messages(tx);
        let mbrlist = Arc::new(Memberlist::with_delegate(delegate, transport_options, opts).await?);

        let cfg = Config::new(Broadcaster::new(mbrlist.clone(), 3));

        let mut broadcast_listener = cfg.broadcast_listener(Subscriber::new(rx));
        tokio::spawn(async move {
            broadcast_listener.run().await;
        });

        Ok((mbrlist, cfg))
    }
}

/// The broadcaster implements transporting data using memberlists send_reliable mode. It
/// grabs all of the online_members when called, and sends the entry to the next num_friends
/// in a sorted list of members. By increasing the number of friends, you increase the likelyhood
/// of the message being delivered faster.
///
/// Copilot generated shade:
///
/// This is a simple implementation, and could be
/// improved by using a more complex algorithm.
///
/// So I guess use with caution.
pub struct Broadcaster<T: Transport> {
    memberlist: Arc<
        Memberlist<T, HermanDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    >,
    num_friends: usize,
}

impl<'a, T: Transport> Broadcaster<T> {
    pub fn new(
        memberlist: Arc<
            Memberlist<T, HermanDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
        >,
        num_friends: usize,
    ) -> Self {
        Self {
            memberlist,
            num_friends,
        }
    }
}

impl<T: Transport, K: Key + Serialize, D: Data + Serialize> Broadcast<K, D> for Broadcaster<T> {
    fn send_entry(&mut self, entry: &Entry<K, D>) -> impl Future<Output = ()> {
        async {
            // grab all the members and sort them
            let mut members: Vec<_> = self.memberlist.online_members().await.to_vec();
            members.sort_by(|a, b| {
                let ahash = BuildHasherDefault::<DefaultHasher>::default().hash_one(&a.id);
                let bhash = BuildHasherDefault::<DefaultHasher>::default().hash_one(&b.id);
                ahash.cmp(&bhash)
            });

            // grab my postion in the list
            let my_id = self.memberlist.local_id();
            let me = match members.iter().position(|m| &m.id == my_id) {
                Some(i) => i,
                None => {
                    tracing::error!(
                        local_addr = %self.memberlist.local_address(),
                        id = %self.memberlist.local_id(),
                        "memberlist.broadcast: failed to find self in memberlist"
                    );
                    return;
                }
            };

            // serialize the message. I really tried to make this generic so you could
            // pass in your own serializer, but serde isn't made that way. correct me if
            // I'm wrong.
            let message = match serde_cbor::to_vec(entry) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        local_addr = %self.memberlist.local_address(),
                        id = %self.memberlist.local_id(),
                        err = %e,
                        "memberlist.broadcast: failed to serialize entry"
                    );
                    return;
                }
            };

            // loop through the number of other nodes we want to talk to, send
            // them each an entry message.
            for index in 0..self.num_friends {
                // grab the next member, wrap around if needed
                let member = &members[(me + index) % members.len()];
                match self
                    .memberlist
                    .send_reliable(&member.addr, message.clone().into())
                    .await
                {
                    Ok(_) => {
                        metrics::counter!("memberlist.packet.sent",).increment(1);
                    }
                    Err(e) => {
                        tracing::debug!(
                            local_addr = %self.memberlist.local_address(),
                            id = %self.memberlist.local_id(),
                            peer_addr = %member,
                            err = %e,
                            "memberlist.broadcast: failed to sen packet"
                        );
                    }
                };
            }
        }
    }
}

pub struct Subscriber {
    messages: Receiver<Bytes>,
}

impl Subscriber {
    pub fn new(messages: Receiver<Bytes>) -> Self {
        Self { messages }
    }
}

pub struct HermanDelegate<K: Id, A: CheapClone + Send + Sync + 'static> {
    phantom: std::marker::PhantomData<(K, A)>,
    messages: Option<Sender<Bytes>>,
}

impl<T: Key + DeserializeOwned, D: Data + DeserializeOwned> Subscribe<T, D> for Subscriber {
    async fn watch(&mut self) -> Entry<T, D> {
        loop {
            match self.messages.recv().await {
                Some(msg) => {
                    let msg = serde_cbor::from_slice::<Entry<T, D>>(&msg);
                    match msg {
                        Ok(entry) => {
                            return entry;
                        }
                        Err(e) => {
                            tracing::error!(err = %e, "failed to deserialize message");
                        }
                    }
                }
                None => {
                    tracing::error!("failed to receive message");
                }
            }
        }
    }
}

impl<K: Id, A: CheapClone + Send + Sync + 'static> HermanDelegate<K, A> {
    pub fn new() -> Self {
        Self {
            phantom: std::marker::PhantomData,
            messages: None,
        }
    }

    pub fn with_messages(messages: Sender<Bytes>) -> Self {
        Self {
            phantom: std::marker::PhantomData,
            messages: Some(messages),
        }
    }

    pub fn set_messages(&mut self, messages: Sender<Bytes>) {
        self.messages = Some(messages);
    }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> AliveDelegate for HermanDelegate<I, A> {
    type Error = VoidDelegateError;
    type Id = I;
    type Address = A;

    async fn notify_alive(
        &self,
        _peer: Arc<NodeState<Self::Id, Self::Address>>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> MergeDelegate for HermanDelegate<I, A> {
    type Error = VoidDelegateError;
    type Id = I;
    type Address = A;

    async fn notify_merge(
        &self,
        _peers: SmallVec<Arc<NodeState<Self::Id, Self::Address>>>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> ConflictDelegate for HermanDelegate<I, A> {
    type Id = I;
    type Address = A;

    async fn notify_conflict(
        &self,
        _existing: Arc<NodeState<Self::Id, Self::Address>>,
        _other: Arc<NodeState<Self::Id, Self::Address>>,
    ) {
    }
}

impl<K: Id, A: CheapClone + Send + Sync + 'static> PingDelegate for HermanDelegate<K, A> {
    type Id = K;
    type Address = A;

    async fn ack_payload(&self) -> Bytes {
        Bytes::new()
    }

    async fn notify_ping_complete(
        &self,
        _node: Arc<NodeState<Self::Id, Self::Address>>,
        _rtt: std::time::Duration,
        _payload: Bytes,
    ) {
    }

    fn disable_promised_pings(&self, _target: &Self::Id) -> bool {
        false
    }
}

impl<K: Id, A: CheapClone + Send + Sync + 'static> NodeDelegate for HermanDelegate<K, A> {
    async fn node_meta(&self, _limit: usize) -> Meta {
        Meta::empty()
    }

    async fn notify_message(&self, msg: Bytes) {
        // we can't really do anything if it doesn't work, so
        // we don't care about the answer, fire and forget.
        if let Some(messages) = &self.messages.as_ref() {
            let _ = messages.send(msg).await;
        }
    }

    async fn broadcast_messages<F>(
        &self,
        _overhead: usize,
        _limit: usize,
        _encoded_len: F,
    ) -> TinyVec<Bytes>
    where
        F: Fn(Bytes) -> (usize, Bytes) + Send,
    {
        TinyVec::new()
    }

    async fn local_state(&self, _join: bool) -> Bytes {
        Bytes::new()
    }

    async fn merge_remote_state(&self, _buf: Bytes, _join: bool) {}
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> EventDelegate for HermanDelegate<I, A> {
    type Id = I;
    type Address = A;

    async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        log::info!("{} joined", node.id);
    }

    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        log::info!("{} left", node.id);
    }

    async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        log::info!("{} updated", node.id);
    }
}

impl<K: Id, A: CheapClone + Send + Sync + 'static> Delegate for HermanDelegate<K, A> {
    type Id = K;
    type Address = A;
}
