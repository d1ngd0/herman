use std::{
    future::Future,
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
};

use memberlist::{net::Transport, Memberlist};
use serde::Serialize;

use super::{
    ledger::{Data, Entry, Key},
    Broadcast,
};

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
    memberlist: Memberlist<T>,
    num_friends: usize,
}

impl<T: Transport> Broadcaster<T> {
    pub fn new(memberlist: Memberlist<T>, num_friends: usize) -> Self {
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
                            "memberlist.broadcast: failed to send packet"
                        );
                    }
                };
            }
        }
    }
}
