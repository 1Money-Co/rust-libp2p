use std::time::Duration;

use futures::{StreamExt, future::Either};
use libp2p_gossipsub::{Behaviour, ConfigBuilder, Event, IdentTopic, MessageAuthenticity};
use libp2p_swarm::{ConnectionId, Swarm, SwarmEvent};
use libp2p_swarm_test::SwarmExt;
use tokio::time::{Instant, sleep_until, timeout};

type Node = Swarm<Behaviour>;

fn node() -> Node {
    Swarm::new_ephemeral_tokio(|key| {
        let config = ConfigBuilder::default()
            .heartbeat_interval(Duration::from_millis(200))
            .build()
            .unwrap();
        let mut behaviour = Behaviour::new(MessageAuthenticity::Signed(key), config).unwrap();
        behaviour.subscribe(&IdentTopic::new("wakeup")).unwrap();
        behaviour
    })
}

async fn drive_until<T>(
    receiver: &mut Node,
    sender: &mut Node,
    mut predicate: impl FnMut(bool, SwarmEvent<Event>) -> Option<T>,
) -> T {
    timeout(Duration::from_secs(5), async {
        loop {
            let (from_sender, event) = match futures::future::select(
                receiver.select_next_some(),
                sender.select_next_some(),
            )
            .await
            {
                Either::Left((event, _)) => (false, event),
                Either::Right((event, _)) => (true, event),
            };
            if let Some(result) = predicate(from_sender, event) {
                return result;
            }
        }
    })
    .await
    .expect("swarm progress timed out")
}

async fn idle(receiver: &mut Node, sender: &mut Node, duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        tokio::select! {
            _ = sleep_until(deadline) => break,
            _ = receiver.select_next_some() => {},
            _ = sender.select_next_some() => {},
        }
    }
}

async fn scenario(pause: Duration) {
    let mut receiver = node();
    let mut sender = node();
    receiver.listen().with_tcp_addr_external().await;
    let address = receiver.external_addresses().next().unwrap().clone();
    let receiver_id = *receiver.local_peer_id();
    sender.dial(address.clone()).unwrap();
    let mut first = None;
    drive_until(&mut receiver, &mut sender, |from_sender, event| {
        match (from_sender, event) {
            (true, SwarmEvent::ConnectionEstablished { connection_id, .. }) => {
                first = Some(connection_id);
                None
            }
            (true, SwarmEvent::Behaviour(Event::Subscribed { peer_id, .. }))
                if peer_id == receiver_id =>
            {
                Some(())
            }
            _ => None,
        }
    })
    .await;
    let first = first.unwrap();
    sender.dial(address).unwrap();
    let survivor = drive_until(&mut receiver, &mut sender, |from_sender, event| {
        match (from_sender, event) {
            (true, SwarmEvent::ConnectionEstablished { connection_id, .. })
                if connection_id != first =>
            {
                Some(connection_id)
            }
            _ => None,
        }
    })
    .await;
    idle(&mut receiver, &mut sender, Duration::from_secs(2)).await;
    assert_eq!(
        sender
            .network_info()
            .connection_counters()
            .num_established(),
        2
    );
    assert!(sender.close_connection(first));
    drive_until(&mut receiver, &mut sender, |from_sender, event| {
        match (from_sender, event) {
            (true, SwarmEvent::ConnectionClosed { connection_id, .. })
                if connection_id == first =>
            {
                Some(())
            }
            _ => None,
        }
    })
    .await;
    idle(&mut receiver, &mut sender, pause).await;
    assert!(sender.is_connected(&receiver_id));
    assert_eq!(
        sender
            .network_info()
            .connection_counters()
            .num_established(),
        1
    );
    sender
        .behaviour_mut()
        .publish(IdentTopic::new("wakeup"), b"after-close".to_vec())
        .unwrap();
    wait_for_message(&mut receiver, &mut sender, survivor, b"after-close").await;
    assert_eq!(
        receiver
            .network_info()
            .connection_counters()
            .num_established(),
        1
    );
    assert_eq!(
        sender
            .network_info()
            .connection_counters()
            .num_established(),
        1
    );
}

async fn wait_for_message(
    receiver: &mut Node,
    sender: &mut Node,
    survivor: ConnectionId,
    payload: &[u8],
) {
    drive_until(receiver, sender, |from_sender, event| {
        match (from_sender, event) {
            (false, SwarmEvent::Behaviour(Event::Message { message, .. })) => {
                assert_eq!(message.data, payload);
                Some(())
            }
            (true, SwarmEvent::ConnectionClosed { connection_id, .. })
                if connection_id == survivor =>
            {
                panic!("surviving connection closed")
            }
            (
                false,
                SwarmEvent::ConnectionClosed {
                    num_established: 0, ..
                },
            ) => {
                panic!("receiver lost its last connection")
            }
            _ => None,
        }
    })
    .await;
}

#[tokio::test]
async fn later_publication_wakes_the_surviving_connection() {
    scenario(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn immediate_publication_after_connection_close_is_delivered() {
    scenario(Duration::ZERO).await;
}
