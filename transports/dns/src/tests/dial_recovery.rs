use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{future::Either, prelude::*};
use hickory_resolver::{
    lookup::Lookup,
    lookup_ip::LookupIp,
    proto::{
        op::Query,
        rr::{Name, RData, Record, RecordType, rdata::A},
    },
};
use libp2p_core::{
    Transport as _,
    multiaddr::{Multiaddr, Protocol},
    upgrade::Version,
};
use libp2p_identity::Keypair;
use libp2p_noise::Config as NoiseConfig;
use libp2p_swarm::{
    Config as SwarmConfig, DialError, Swarm, SwarmEvent, dial_opts::DialOpts, dummy,
};
use libp2p_tcp::tokio::Transport as TcpTransport;
use libp2p_yamux::Config as YamuxConfig;
use parking_lot::Mutex;

use super::{ResolveError, Resolver, Transport};

#[derive(Clone, Default)]
struct AuthorityOnlyResolver {
    answer_usable: Arc<AtomicBool>,
}

impl Resolver for AuthorityOnlyResolver {
    async fn lookup_ip(&self, name: String) -> Result<LookupIp, ResolveError> {
        Ok(self.ipv4_lookup(name).await?.into())
    }

    async fn ipv4_lookup(&self, name: String) -> Result<Lookup, ResolveError> {
        let name = Name::from_ascii(name).unwrap();
        let query = Query::query(name.clone(), RecordType::A);
        let record = Record::from_rdata(name, 60, RData::A(A::new(127, 0, 0, 1)));
        if self.answer_usable.load(Ordering::SeqCst) {
            Ok(Lookup::new_with_max_ttl(query, [record]))
        } else {
            let mut lookup = Lookup::new_with_max_ttl(query, []);
            lookup.extend_authorities([record]);
            Ok(lookup)
        }
    }

    async fn ipv6_lookup(&self, _: String) -> Result<Lookup, ResolveError> {
        unreachable!()
    }

    async fn txt_lookup(&self, _: String) -> Result<Lookup, ResolveError> {
        unreachable!()
    }
}

#[test]
fn authority_only_lookup_reports_failure_and_allows_redial() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let resolver = AuthorityOnlyResolver::default();
            let mut dialer = swarm(resolver.clone());
            let mut listener = swarm(AuthorityOnlyResolver::default());
            listener
                .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
                .unwrap();
            let address = loop {
                if let SwarmEvent::NewListenAddr { address, .. } = listener.select_next_some().await
                {
                    break address;
                }
            };
            let port = address
                .iter()
                .find_map(|p| match p {
                    Protocol::Tcp(port) => Some(port),
                    _ => None,
                })
                .unwrap();
            let address: Multiaddr = format!("/dns4/example.com/tcp/{port}").parse().unwrap();
            let peer_id = *listener.local_peer_id();
            dialer
                .dial(
                    DialOpts::peer_id(peer_id)
                        .addresses(vec![address.clone()])
                        .build(),
                )
                .unwrap();
            loop {
                if let SwarmEvent::OutgoingConnectionError {
                    peer_id: failed_peer,
                    error,
                    ..
                } = dialer.select_next_some().await
                {
                    assert_eq!(failed_peer, Some(peer_id));
                    let DialError::Transport(errors) = error else {
                        panic!("expected a transport dial failure: {error:?}");
                    };
                    assert_eq!(errors.len(), 1);
                    assert!(
                        format!("{:?}", errors[0].1).contains("NoRecordsFound"),
                        "{:?}",
                        errors[0].1
                    );
                    break;
                }
            }
            resolver.answer_usable.store(true, Ordering::SeqCst);
            // The default peer condition rejects a retry if the previous dial is still pending.
            dialer
                .dial(DialOpts::peer_id(peer_id).addresses(vec![address]).build())
                .unwrap();
            loop {
                match futures::future::select(
                    dialer.select_next_some(),
                    listener.select_next_some(),
                )
                .await
                {
                    Either::Left((
                        SwarmEvent::ConnectionEstablished {
                            peer_id: connected, ..
                        },
                        _,
                    )) => {
                        assert_eq!(connected, peer_id);
                        break;
                    }
                    Either::Left((SwarmEvent::OutgoingConnectionError { error, .. }, _)) => {
                        panic!("redial failed: {error:?}")
                    }
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
    });
}

fn swarm(resolver: AuthorityOnlyResolver) -> Swarm<dummy::Behaviour> {
    let key = Keypair::generate_ed25519();
    let transport = Transport {
        inner: Arc::new(Mutex::new(TcpTransport::new(Default::default()))),
        resolver,
    }
    .upgrade(Version::V1Lazy)
    .authenticate(NoiseConfig::new(&key).unwrap())
    .multiplex(YamuxConfig::default())
    .boxed();
    Swarm::new(
        transport,
        dummy::Behaviour,
        key.public().to_peer_id(),
        SwarmConfig::with_tokio_executor(),
    )
}
