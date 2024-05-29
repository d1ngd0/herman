use std::env;
use std::hash::{DefaultHasher, Hasher};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use dapt::Dapt;
use herman::ccfg::{HermanDelegate, WithHermanDelegate};
use herman::Config;
use memberlist::agnostic::tokio::TokioRuntime;
use memberlist::net::resolver::address::NodeAddressResolver;
use memberlist::net::stream_layer::tcp::Tcp;
use memberlist::net::{
    Lpe, MaybeResolvedAddress, NetTransport, NetTransportOptions, Node, NodeAddress,
};
use memberlist::{Memberlist, Options};

use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    log::set_logger(&SimpleLogger).unwrap();
    log::set_max_level(log::LevelFilter::Info);

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

    let (m, _cfg): (
        Arc<
            Memberlist<
                NetTransport<
                    u64,
                    NodeAddressResolver<TokioRuntime>,
                    Tcp<TokioRuntime>,
                    Lpe<_, _>,
                    TokioRuntime,
                >,
                HermanDelegate<u64, SocketAddr>,
            >,
        >,
        Config<u64, Dapt, _>,
    ) = Memberlist::with_config(transport_options, Options::default())
        .await
        .unwrap();

    for member_addr in args {
        let member_addr_hash = {
            let mut hasher = DefaultHasher::new();
            hasher.write(member_addr.as_bytes());
            hasher.finish()
        };

        let _node = m
            .join(Node::new(
                member_addr_hash,
                MaybeResolvedAddress::Unresolved(
                    NodeAddress::from_str(member_addr.as_str()).expect("invalid member address"),
                ),
            ))
            .await?;
    }

    signal::ctrl_c().await.expect("failed to listen for event");

    println!("{:?}", m.members().await);

    Ok(())
}

use log::{Level, Metadata, Record};

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}
