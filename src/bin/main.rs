use std::env;
use std::hash::{DefaultHasher, Hasher};
use std::str::FromStr;

use memberlist::agnostic::tokio::TokioRuntime;
use memberlist::net::resolver::address::NodeAddressResolver;
use memberlist::net::stream_layer::tcp::Tcp;
use memberlist::net::{
    Lpe, MaybeResolvedAddress, NetTransport, NetTransportOptions, Node, NodeAddress,
};
use memberlist::{Memberlist, Options};

use tokio::signal;

#[tokio::main]
async fn main() {
    let mut args = env::args();
    let _ = args.next(); // the first argument is the name of the binary
    let bind_address = args.next().expect("missing bind address");
    let bind_hash = {
        let mut hasher = DefaultHasher::new();
        hasher.write(bind_address.as_bytes());
        hasher.finish()
    };

    let mut transport_options: NetTransportOptions<
        u64,
        NodeAddressResolver<TokioRuntime>,
        Tcp<TokioRuntime>,
    > = NetTransportOptions::new(bind_hash);

    transport_options.add_bind_address(
        NodeAddress::from_str(bind_address.as_str()).expect("invalid bind address"),
    );

    let (tx, rx) = tokio::sync::mpsc::channel(100);

    let messages = Subscriber::new(rx);

    let delegate = HermesDelegate::with_messages(tx);

    let m: Memberlist<
        NetTransport<
            u64,
            NodeAddressResolver<TokioRuntime>,
            Tcp<TokioRuntime>,
            Lpe<_, _>,
            TokioRuntime,
        >,
    > = Memberlist::with_delegate(delegate, transport_options, Options::default())
        .await
        .unwrap();

    for member_addr in args {
        let member_addr_hash = {
            let mut hasher = DefaultHasher::new();
            hasher.write(member_addr.as_bytes());
            hasher.finish()
        };

        let _ = m
            .join(Node::new(
                member_addr_hash,
                MaybeResolvedAddress::Unresolved(
                    NodeAddress::from_str(member_addr.as_str()).expect("invalid member address"),
                ),
            ))
            .await;
    }

    signal::ctrl_c().await.expect("failed to listen for event");

    println!("{:?}", m.members().await);
}
