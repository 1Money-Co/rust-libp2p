use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use asynchronous_codec::Framed;
use futures::{AsyncWriteExt, Sink, StreamExt};
use futures_timer::Delay;
use libp2p_stream::Behaviour as StreamBehaviour;
use libp2p_swarm::{Stream, StreamProtocol, Swarm};
use libp2p_swarm_test::SwarmExt;

use super::*;
use crate::{TopicHash, ValidationMode, types::Graft};

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn publication(data: &[u8]) -> RpcOut {
    RpcOut::Publish {
        message_id: data.to_vec().into(),
        message: RawMessage {
            source: None,
            data: data.to_vec(),
            sequence_number: None,
            topic: TopicHash::from_raw("wakeup"),
            signature: None,
            key: None,
            validated: true,
        },
        timeout: Delay::new(Duration::from_secs(60)),
    }
}

fn handler(queue: Queue) -> Handler {
    Handler::new(PeerId::random(), ProtocolConfig::default(), queue)
}

fn queued_messages() -> [RpcOut; 3] {
    [
        RpcOut::Subscribe {
            topic: TopicHash::from_raw("wakeup"),
            requests_partial: false,
            supports_partial: false,
        },
        RpcOut::Graft(Graft {
            topic_hash: TopicHash::from_raw("wakeup"),
        }),
        publication(b"publish"),
    ]
}

#[test]
fn idle_handler_registers_a_waker_for_every_queue_lane() {
    for message in queued_messages() {
        let mut queue = Queue::new(8);
        let mut handler = handler(queue.clone());
        let count = Arc::new(WakeCount::default());
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(handler.poll(&mut cx).is_pending());
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        queue.try_push(message).unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));
        assert!(
            queue.try_pop().is_some(),
            "readiness must not consume the message"
        );
        assert!(
            handler.poll(&mut cx).is_pending(),
            "negotiation must not be requested twice"
        );
    }
}

#[test]
fn idle_handler_replaces_a_registered_waker() {
    let mut queue = Queue::new(8);
    let mut handler = handler(queue.clone());
    let old = Arc::new(WakeCount::default());
    let current = Arc::new(WakeCount::default());
    for count in [&old, &current] {
        let waker = Waker::from(count.clone());
        assert!(handler.poll(&mut Context::from_waker(&waker)).is_pending());
    }
    queue.try_push(publication(b"later")).unwrap();
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(current.0.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_an_idle_sibling_keeps_the_survivor_registered() {
    let mut queue = Queue::new(8);
    let mut older = handler(queue.clone());
    let mut survivor = handler(queue.clone());
    let older_count = Arc::new(WakeCount::default());
    let survivor_count = Arc::new(WakeCount::default());
    for (handler, count) in [(&mut older, &older_count), (&mut survivor, &survivor_count)] {
        let waker = Waker::from(count.clone());
        assert!(handler.poll(&mut Context::from_waker(&waker)).is_pending());
    }
    drop(older);
    queue.try_push(publication(b"survivor")).unwrap();
    assert_eq!(older_count.0.load(Ordering::SeqCst), 0);
    assert_eq!(survivor_count.0.load(Ordering::SeqCst), 1);
    let waker = Waker::from(survivor_count);
    assert!(matches!(
        survivor.poll(&mut Context::from_waker(&waker)),
        Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
    ));
}

#[test]
fn retries_keep_the_existing_attempt_limit() {
    let mut queue = Queue::new(8);
    let mut handler = handler(queue.clone());
    queue.try_push(publication(b"retry")).unwrap();
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..MAX_SUBSTREAM_ATTEMPTS {
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));
        assert!(handler.poll(&mut cx).is_pending());
        handler.on_connection_event(ConnectionEvent::DialUpgradeError(DialUpgradeError {
            info: (),
            error: StreamUpgradeError::Io(io::Error::other("injected negotiation failure")),
        }));
    }
    assert!(matches!(
        handler,
        Handler::Disabled(DisabledHandler::MaxSubstreamAttempts)
    ));
    assert!(handler.poll(&mut cx).is_pending());
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Ready,
    Send,
    Flush,
}

async fn failed_stream(failure: Failure) -> OutboundSubstreamState {
    let mut sender = Swarm::new_ephemeral_tokio(|_| StreamBehaviour::new());
    let mut receiver = Swarm::new_ephemeral_tokio(|_| StreamBehaviour::new());
    let protocol = StreamProtocol::new("/test/gossip-wakeup");
    let mut control = sender.behaviour().new_control();
    let mut incoming = receiver
        .behaviour()
        .new_control()
        .accept(protocol.clone())
        .unwrap();
    receiver.listen().with_memory_addr_external().await;
    sender.connect(&mut receiver).await;
    let receiver_id = *receiver.local_peer_id();
    let sender_task = tokio::spawn(sender.loop_on_next());
    let receiver_task = tokio::spawn(receiver.loop_on_next());
    let mut stream: Stream = control.open_stream(receiver_id, protocol).await.unwrap();
    let (_, remote) = incoming.next().await.unwrap();
    stream.close().await.unwrap();
    drop(remote);
    sender_task.abort();
    receiver_task.abort();
    let _ = sender_task.await;
    let _ = receiver_task.await;
    let limit = if matches!(failure, Failure::Send) {
        0
    } else {
        65_536
    };
    let codec = GossipsubCodec::new(limit, ValidationMode::Anonymous, HashMap::new(), 5000, 5000);
    let mut framed = Framed::new(stream, codec);
    let message = publication(b"failed").into_protobuf();
    match failure {
        Failure::Send => OutboundSubstreamState::PendingSend(framed, Box::new(message)),
        Failure::Ready | Failure::Flush => {
            Sink::start_send(Pin::new(&mut framed), message.clone()).unwrap();
            if matches!(failure, Failure::Ready) {
                framed.set_send_high_water_mark(1);
                OutboundSubstreamState::PendingSend(framed, Box::new(message))
            } else {
                OutboundSubstreamState::PendingFlush(framed)
            }
        }
    }
}

async fn assert_failure_recovery(queued: bool) {
    for failure in [Failure::Ready, Failure::Send, Failure::Flush] {
        let stream = tokio::time::timeout(Duration::from_secs(10), failed_stream(failure))
            .await
            .unwrap();
        let mut queue = Queue::new(8);
        let mut handler = handler(queue.clone());
        let Handler::Enabled(enabled) = &mut handler else {
            unreachable!()
        };
        enabled.outbound_substream = Some(stream);
        if queued {
            queue.try_push(publication(b"queued")).unwrap();
        }
        let count = Arc::new(WakeCount::default());
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);
        let result = handler.poll(&mut cx);
        let Handler::Enabled(enabled) = &handler else {
            unreachable!()
        };
        assert!(
            enabled.outbound_substream.is_none(),
            "{failure:?} must fail its stream operation"
        );
        if queued {
            assert!(
                matches!(
                    result,
                    Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
                ),
                "{failure:?} must request a replacement immediately"
            );
        } else {
            assert!(result.is_pending());
            queue.try_push(publication(b"after-failure")).unwrap();
            assert_eq!(
                count.0.load(Ordering::SeqCst),
                1,
                "{failure:?} must wake on a later publication"
            );
            assert!(matches!(
                handler.poll(&mut cx),
                Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
            ));
        }
        assert!(
            handler.poll(&mut cx).is_pending(),
            "{failure:?} must not spin while negotiating"
        );
        assert!(queue.try_pop().is_some());
    }
}

#[tokio::test]
async fn stream_errors_request_replacement_for_queued_work() {
    assert_failure_recovery(true).await;
}

#[tokio::test]
async fn stream_errors_register_wakeups_for_later_publications() {
    assert_failure_recovery(false).await;
}
