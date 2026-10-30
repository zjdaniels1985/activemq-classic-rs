use std::time::Duration;

use activemq_classic_rs::config::{AppConfig, ListenerConfig, NetworkConnectorConfig};
use activemq_classic_rs::core::security::{AuthorizationEntry, UserConfig};
use activemq_classic_rs::core::{Body, Destination, Message};
use activemq_classic_rs::protocols::openwire::{OpenWireClient, OpenWireFrame};

static NETWORK_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn broker_config() -> AppConfig {
    let mut config = AppConfig::default();
    config.protocols.openwire = ListenerConfig::new(true, "127.0.0.1:0");
    config.protocols.stomp.enabled = false;
    config.protocols.amqp.enabled = false;
    config.protocols.mqtt.enabled = false;
    config.persistence.backend = "memory".to_owned();
    config.web_console.enabled = false;
    config
}

async fn connected(address: std::net::SocketAddr) -> OpenWireClient {
    OpenWireClient::connect_client(address, 8 * 1024 * 1024, None, None, None)
        .await
        .unwrap()
}

async fn send(client: &mut OpenWireClient, destination: &Destination, text: &str) -> String {
    let producer = client.create_producer(Some(destination)).await.unwrap();
    let message = Message::text(destination.clone(), text).with_property("network-test", text);
    let id = message.message_id.clone();
    client.send(&producer, &message).await.unwrap();
    id
}

async fn create_temp_queue(client: &mut OpenWireClient, name: &str) -> Destination {
    let destination = Destination::temp_queue(name);
    let command_id = client.next_command_id();
    let mut payload = command_id.to_be_bytes().to_vec();
    payload.push(1); // response required
    payload.push(0); // DestinationInfo connection id
    payload.extend_from_slice(&[1, 102, 1]); // temporary queue destination
    payload.extend_from_slice(&(name.len() as u16).to_be_bytes());
    payload.extend_from_slice(name.as_bytes());
    payload.push(0); // add
    payload.extend_from_slice(&0i64.to_be_bytes());
    payload.push(0); // no broker path
    client
        .send_command(&OpenWireFrame {
            command_type: 8,
            payload,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = client.receive().await.unwrap();
            if frame.command_type == 30
                && frame.payload.get(5..9) == Some(command_id.to_be_bytes().as_slice())
            {
                break;
            }
            assert_ne!(
                frame.command_type, 31,
                "temporary destination creation failed"
            );
        }
    })
    .await
    .expect("temporary destination creation response timed out");
    destination
}

#[tokio::test]
async fn stopping_broker_closes_bridge_connections_and_consumers() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let mut config = broker_config();
    config.network_connectors.push(NetworkConnectorConfig {
        name: "shutdown".into(),
        uris: vec![format!("tcp://{}", remote.address("openwire").unwrap())],
        static_included_destinations: vec!["queue://shutdown.jobs".into()],
        reconnect_delay_ms: 100,
        ..NetworkConnectorConfig::default()
    });
    let local = activemq_classic_rs::runtime::start(config).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while remote.core.connections().is_empty() || local.core.consumers().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bridge did not connect");
    let core = local.core.clone();
    local.stop();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !remote.core.connections().is_empty() || !core.consumers().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "stopped bridge retained connections {:?} or consumers {:?}",
            remote.core.connections(),
            core.consumers()
        )
    });
    remote.stop();
}

#[tokio::test]
async fn static_and_demand_queue_topic_bridges_apply_exclusions_and_keep_idle_queues_local() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let remote_address = remote.address("openwire").unwrap();

    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "test-bridge".into(),
            uris: vec![format!("tcp://{remote_address}")],
            static_included_destinations: vec!["queue://static.jobs".into()],
            dynamically_included_destinations: vec![
                "queue://demand.>".into(),
                "topic://news.>".into(),
            ],
            excluded_destinations: vec!["queue://demand.private.>".into()],
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let mut remote_client = connected(remote_address).await;
    let mut local_client = connected(local.address("openwire").unwrap()).await;

    let static_destination = Destination::queue("static.jobs");
    let static_consumer = remote_client
        .create_consumer(&static_destination, 32, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    send(&mut local_client, &static_destination, "static").await;
    let received = remote_client
        .receive_message(Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.0, static_consumer);
    assert_eq!(received.1.body, Body::Text("static".into()));
    assert_eq!(
        received
            .1
            .properties
            .get("network-test")
            .and_then(|value| value.as_str()),
        Some("static")
    );
    remote_client
        .acknowledge(&static_consumer, &received.1.message_id)
        .await
        .unwrap();

    let demand_destination = Destination::queue("demand.orders");
    let demand_consumer = remote_client
        .create_consumer(&demand_destination, 32, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    send(&mut local_client, &demand_destination, "demand").await;
    let received = remote_client
        .receive_message(Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.0, demand_consumer);
    assert_eq!(received.1.body, Body::Text("demand".into()));
    remote_client
        .acknowledge(&demand_consumer, &received.1.message_id)
        .await
        .unwrap();

    let topic = Destination::topic("news.world");
    let topic_consumer = remote_client
        .create_consumer(&topic, 32, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    send(&mut local_client, &topic, "topic").await;
    let received = remote_client
        .receive_message(Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.0, topic_consumer);
    assert_eq!(received.1.body, Body::Text("topic".into()));
    remote_client
        .acknowledge(&topic_consumer, &received.1.message_id)
        .await
        .unwrap();

    let excluded = Destination::queue("demand.private.audit");
    let _excluded_consumer = remote_client
        .create_consumer(&excluded, 32, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    send(&mut local_client, &excluded, "excluded").await;
    assert!(
        remote_client
            .receive_message(Duration::from_millis(300))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        local
            .core
            .browse(&excluded, 10)
            .unwrap()
            .first()
            .map(|message| message.body.as_str()),
        Some("excluded")
    );
    let idle = Destination::queue("demand.idle");
    send(&mut local_client, &idle, "local-only").await;
    assert!(
        remote_client
            .receive_message(Duration::from_millis(300))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        local
            .core
            .browse(&idle, 10)
            .unwrap()
            .first()
            .map(|message| message.body.as_str()),
        Some("local-only")
    );

    local.stop();
    remote.stop();
}

#[tokio::test]
async fn removing_remote_demand_closes_the_local_conduit_consumer() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let remote_address = remote.address("openwire").unwrap();

    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "demand-removal".into(),
            uris: vec![format!("tcp://{remote_address}")],
            dynamically_included_destinations: vec!["queue://demand.removal".into()],
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let mut remote_client = connected(remote_address).await;
    let destination = Destination::queue("demand.removal");
    let remote_consumer = remote_client
        .create_consumer(&destination, 32, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let command_id = remote_client.next_command_id();
    let mut payload = command_id.to_be_bytes().to_vec();
    payload.push(0);
    payload.extend_from_slice(&[1, 122, 1]);
    let encoded_id = remote_consumer.connection.as_bytes();
    payload.extend_from_slice(&(encoded_id.len() as u16).to_be_bytes());
    payload.extend_from_slice(encoded_id);
    payload.extend_from_slice(&remote_consumer.session.to_be_bytes());
    payload.extend_from_slice(&remote_consumer.value.to_be_bytes());
    remote_client
        .send_command(&activemq_classic_rs::protocols::openwire::OpenWireFrame {
            command_type: 12,
            payload,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while local
            .core
            .consumers()
            .iter()
            .any(|consumer| consumer.network && consumer.destination == destination.to_uri())
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("remote consumer removal should close the local network consumer");

    let mut local_client = connected(local.address("openwire").unwrap()).await;
    send(&mut local_client, &destination, "after-demand-removal").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        local
            .core
            .browse(&destination, 10)
            .unwrap()
            .first()
            .map(|message| message.body.as_str()),
        Some("after-demand-removal")
    );

    local.stop();
    remote.stop();
}

#[tokio::test]
async fn broker_path_prevents_a_static_two_broker_loop() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_address = probe.local_addr().unwrap();
    drop(probe);

    let mut b_config = broker_config();
    b_config.network_connectors.push(NetworkConnectorConfig {
        name: "b-to-a".into(),
        uris: vec![format!("tcp://{a_address}")],
        static_included_destinations: vec!["queue://loop.test".into()],
        network_ttl: 1,
        reconnect_delay_ms: 100,
        ..NetworkConnectorConfig::default()
    });
    let broker_b = activemq_classic_rs::runtime::start(b_config).await.unwrap();
    let b_address = broker_b.address("openwire").unwrap();

    let mut a_config = broker_config();
    a_config.protocols.openwire.bind = a_address.to_string();
    a_config.network_connectors.push(NetworkConnectorConfig {
        name: "a-to-b".into(),
        uris: vec![format!("tcp://{b_address}")],
        static_included_destinations: vec!["queue://loop.test".into()],
        network_ttl: 1,
        reconnect_delay_ms: 100,
        ..NetworkConnectorConfig::default()
    });
    let broker_a = activemq_classic_rs::runtime::start(a_config).await.unwrap();
    let destination = Destination::queue("loop.test");

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let (internal, _events) = broker_a.core.connect_internal("loop-test");
    let mut message = Message::text(destination.clone(), "already-visited");
    message
        .broker_path
        .push(broker_a.core.broker_id().to_owned());
    broker_a.core.send(internal, message, None).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(broker_a.core.browse(&destination, 10).unwrap().len(), 1);
    broker_a
        .core
        .send(
            internal,
            Message::text(destination.clone(), "ttl-limited"),
            None,
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while broker_b.core.browse(&destination, 10).unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("first hop should be forwarded to broker B");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(broker_a.core.browse(&destination, 10).unwrap().len(), 1);
    assert_eq!(broker_b.core.browse(&destination, 10).unwrap().len(), 1);

    broker_a.stop();
    broker_b.stop();
}

#[tokio::test]
async fn static_bridge_retries_until_remote_becomes_available() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote_address = probe.local_addr().unwrap();
    drop(probe);

    let mut remote_config = broker_config();
    remote_config.protocols.openwire.bind = remote_address.to_string();
    remote_config.security.enabled = true;
    remote_config.security.allow_anonymous = false;
    remote_config.security.users.push(UserConfig {
        username: "bridge-user".into(),
        password: Some("bridge-secret".into()),
        groups: vec!["bridges".into()],
        ..UserConfig::default()
    });
    remote_config
        .security
        .authorization
        .push(AuthorizationEntry {
            destination: "queue://>".into(),
            read: vec!["bridges".into()],
            write: vec!["bridges".into()],
            admin: vec!["bridges".into()],
        });
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "reconnect".into(),
            uris: vec![
                "tcp://127.0.0.1:1".into(),
                format!("tcp://{remote_address}"),
            ],
            username: Some("bridge-user".into()),
            password: Some("bridge-secret".into()),
            static_included_destinations: vec!["queue://restart.jobs".into()],
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let remote = activemq_classic_rs::runtime::start(remote_config)
        .await
        .unwrap();
    let mut remote_client = OpenWireClient::connect_client(
        remote_address,
        8 * 1024 * 1024,
        None,
        Some("bridge-user"),
        Some("bridge-secret"),
    )
    .await
    .unwrap();
    let destination = Destination::queue("restart.jobs");
    let remote_consumer = remote_client
        .create_consumer(&destination, 10, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let mut local_client = connected(local.address("openwire").unwrap()).await;
    send(&mut local_client, &destination, "after-reconnect").await;
    let received = remote_client
        .receive_message(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("message should be forwarded after reconnect");
    assert_eq!(received.0, remote_consumer);
    assert_eq!(received.1.body, Body::Text("after-reconnect".into()));

    local.stop();
    remote.stop();
}

#[tokio::test]
async fn bridge_reconnects_after_established_remote_broker_restarts() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote_address = probe.local_addr().unwrap();
    drop(probe);

    let mut remote_config = broker_config();
    remote_config.protocols.openwire.bind = remote_address.to_string();
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "established-reconnect".into(),
            uris: vec![format!("tcp://{remote_address}")],
            static_included_destinations: vec!["queue://restart.established".into()],
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let first_remote = activemq_classic_rs::runtime::start(remote_config.clone())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while first_remote.core.connections().is_empty() || local.core.consumers().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("initial bridge connection was not established");
    for connection in first_remote.core.connections() {
        first_remote.core.close_connection(connection.id);
    }
    first_remote.stop();
    tokio::time::timeout(Duration::from_secs(5), async {
        while local
            .core
            .consumers()
            .iter()
            .any(|consumer| consumer.network)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("local network consumers were not removed after remote shutdown");

    let restarted_remote = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match activemq_classic_rs::runtime::start(remote_config.clone()).await {
                Ok(broker) => break broker,
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => panic!("could not restart remote broker: {error}"),
            }
        }
    })
    .await
    .expect("remote listener did not release its address for restart");
    let destination = Destination::queue("restart.established");
    let mut remote_client = connected(remote_address).await;
    let remote_consumer = remote_client
        .create_consumer(&destination, 10, None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while restarted_remote.core.connections().len() < 2
            || !local
                .core
                .consumers()
                .iter()
                .any(|consumer| consumer.network && consumer.destination == destination.to_uri())
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bridge did not reconnect to the restarted broker");

    let mut local_client = connected(local.address("openwire").unwrap()).await;
    send(&mut local_client, &destination, "after-established-restart").await;
    let received = remote_client
        .receive_message(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("message should pass through the re-established bridge");
    assert_eq!(received.0, remote_consumer);
    assert_eq!(
        received.1.body,
        Body::Text("after-established-restart".into())
    );

    local.stop();
    restarted_remote.stop();
}

#[tokio::test]
async fn non_conduit_bridges_keep_one_local_consumer_per_remote_demand() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let destination = Destination::queue("demand.nonconduit");
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "non-conduit".into(),
            uris: vec![format!("tcp://{}", remote.address("openwire").unwrap())],
            dynamically_included_destinations: vec![destination.to_uri()],
            conduit_subscriptions: false,
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let mut remote_client = connected(remote.address("openwire").unwrap()).await;
    let first = remote_client
        .create_consumer(&destination, 10, None)
        .await
        .unwrap();
    let second = remote_client
        .create_consumer(&destination, 10, None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while local
            .core
            .consumers()
            .iter()
            .filter(|consumer| consumer.network && consumer.destination == destination.to_uri())
            .count()
            < 2
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("each remote consumer should create its own network consumer");

    let mut local_client = connected(local.address("openwire").unwrap()).await;
    send(&mut local_client, &destination, "non-conduit").await;
    let received = remote_client
        .receive_message(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("message should be forwarded to one remote consumer");
    assert!(received.0 == first || received.0 == second);
    assert_eq!(received.1.body, Body::Text("non-conduit".into()));

    local.stop();
    remote.stop();
}

#[tokio::test]
async fn remote_temporary_queue_is_proxied_and_cleaned_up_with_owner_lifecycle() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "temporary-forward".into(),
            uris: vec![format!("tcp://{}", remote.address("openwire").unwrap())],
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let mut remote_client = connected(remote.address("openwire").unwrap()).await;
    let name = format!("ID:network-temp-{}", std::process::id());
    let destination = create_temp_queue(&mut remote_client, &name).await;
    let remote_consumer = remote_client
        .create_consumer(&destination, 10, None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !local
            .core
            .destinations()
            .iter()
            .any(|entry| entry.destination == destination)
            || !local
                .core
                .consumers()
                .iter()
                .any(|consumer| consumer.network && consumer.destination == destination.to_uri())
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bridge did not create a temporary destination proxy");

    let mut local_client = connected(local.address("openwire").unwrap()).await;
    send(&mut local_client, &destination, "temporary-message").await;
    let received = remote_client
        .receive_message(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("temporary destination message was not forwarded");
    assert_eq!(received.0, remote_consumer);
    assert_eq!(received.1.body, Body::Text("temporary-message".into()));

    drop(remote_client);
    tokio::time::timeout(Duration::from_secs(5), async {
        while local
            .core
            .destinations()
            .iter()
            .any(|entry| entry.destination == destination)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("temporary destination proxy outlived its remote owner demand");

    local.stop();
    remote.stop();
}

async fn wait_for_network_consumer(
    broker: &activemq_classic_rs::runtime::RunningBroker,
    destination: &Destination,
    present: bool,
    context: &str,
) {
    let uri = destination.to_uri();
    tokio::time::timeout(Duration::from_secs(10), async {
        while broker
            .core
            .consumers()
            .iter()
            .any(|consumer| consumer.network && consumer.destination == uri)
            != present
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{context}: network consumer for {uri} present != {present}"));
}

async fn expect_text(client: &mut OpenWireClient, text: &str) -> Message {
    let (_, message, _) = client
        .receive_message(Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("expected {text}"));
    assert_eq!(message.body, Body::Text(text.into()));
    message
}

async fn expect_silence(client: &mut OpenWireClient, context: &str) {
    if let Some((_, message, _)) = client
        .receive_message(Duration::from_millis(700))
        .await
        .unwrap()
    {
        panic!("{context}: unexpected extra delivery {:?}", message.body);
    }
}

#[tokio::test]
async fn duplex_bridge_forwards_queue_and_topic_demand_in_both_directions_without_loops() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let remote_address = remote.address("openwire").unwrap();
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "duplex".into(),
            uris: vec![format!("tcp://{remote_address}")],
            dynamically_included_destinations: vec![
                "queue://dx.>".into(),
                "topic://dxnews.>".into(),
            ],
            duplex: true,
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let mut remote_client = connected(remote_address).await;
    let mut local_client = connected(local.address("openwire").unwrap()).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while remote.core.connections().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("duplex bridge did not connect");

    // Remote demand pulls local messages (forward direction).
    let forward = Destination::queue("dx.forward");
    remote_client
        .create_consumer(&forward, 10, None)
        .await
        .unwrap();
    wait_for_network_consumer(&local, &forward, true, "forward demand").await;
    send(&mut local_client, &forward, "forward-1").await;
    let forwarded = expect_text(&mut remote_client, "forward-1").await;
    assert_eq!(forwarded.broker_path, vec![local.core.info().id]);

    // Local demand is mirrored as a network consumer on the stock remote.
    let reverse = Destination::queue("dx.reverse");
    let reverse_consumer = local_client
        .create_consumer(&reverse, 10, None)
        .await
        .unwrap();
    wait_for_network_consumer(&remote, &reverse, true, "reverse demand").await;
    send(&mut remote_client, &reverse, "reverse-1").await;
    let reversed = expect_text(&mut local_client, "reverse-1").await;
    assert_eq!(reversed.broker_path, vec![remote.core.info().id]);
    assert_eq!(
        remote
            .core
            .destinations()
            .iter()
            .filter(|d| d.destination == reverse)
            .map(|d| d.pending)
            .sum::<usize>(),
        0
    );
    expect_silence(&mut local_client, "reverse queue").await;
    expect_silence(&mut remote_client, "forward queue").await;

    // Topic subscribers on both brokers each receive exactly one copy.
    let topic = Destination::topic("dxnews.eu");
    local_client
        .create_consumer(&topic, 10, None)
        .await
        .unwrap();
    remote_client
        .create_consumer(&topic, 10, None)
        .await
        .unwrap();
    wait_for_network_consumer(&local, &topic, true, "topic forward demand").await;
    wait_for_network_consumer(&remote, &topic, true, "topic reverse demand").await;
    send(&mut local_client, &topic, "from-local").await;
    expect_text(&mut local_client, "from-local").await;
    expect_text(&mut remote_client, "from-local").await;
    send(&mut remote_client, &topic, "from-remote").await;
    expect_text(&mut remote_client, "from-remote").await;
    expect_text(&mut local_client, "from-remote").await;
    expect_silence(&mut local_client, "topic loop on local").await;
    expect_silence(&mut remote_client, "topic loop on remote").await;

    // Removing the local consumer withdraws the mirrored remote consumer.
    local_client
        .remove_consumer(&reverse_consumer)
        .await
        .unwrap();
    wait_for_network_consumer(&remote, &reverse, false, "reverse demand removal").await;
    send(&mut remote_client, &reverse, "stays-remote").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        remote
            .core
            .destinations()
            .iter()
            .any(|d| d.destination == reverse && d.pending == 1),
        "message without local demand must stay on the remote broker"
    );

    let core = local.core.clone();
    local.stop();
    tokio::time::timeout(Duration::from_secs(10), async {
        while remote.core.connections().len() > 1 || core.consumers().iter().any(|c| c.network) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "stopped duplex bridge retained remote connections {:?}",
            remote.core.connections()
        )
    });
    remote.stop();
}

#[tokio::test]
async fn duplex_reverse_demand_is_restored_after_remote_restart() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote_address = probe.local_addr().unwrap();
    drop(probe);
    let mut remote_config = broker_config();
    remote_config.protocols.openwire.bind = remote_address.to_string();
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "duplex-restart".into(),
            uris: vec![format!("tcp://{remote_address}")],
            dynamically_included_destinations: vec!["queue://dxr.>".into()],
            duplex: true,
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let destination = Destination::queue("dxr.jobs");
    let mut local_client = connected(local.address("openwire").unwrap()).await;
    local_client
        .create_consumer(&destination, 10, None)
        .await
        .unwrap();
    let first_remote = activemq_classic_rs::runtime::start(remote_config.clone())
        .await
        .unwrap();
    wait_for_network_consumer(&first_remote, &destination, true, "initial duplex demand").await;
    for connection in first_remote.core.connections() {
        first_remote.core.close_connection(connection.id);
    }
    first_remote.stop();

    let restarted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match activemq_classic_rs::runtime::start(remote_config.clone()).await {
                Ok(broker) => break broker,
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => panic!("could not restart remote broker: {error}"),
            }
        }
    })
    .await
    .expect("remote listener did not release its address for restart");
    wait_for_network_consumer(&restarted, &destination, true, "restored duplex demand").await;
    let mut remote_client = connected(remote_address).await;
    send(&mut remote_client, &destination, "after-duplex-restart").await;
    expect_text(&mut local_client, "after-duplex-restart").await;
    expect_silence(&mut local_client, "duplex restart duplicate").await;

    local.stop();
    restarted.stop();
}

#[tokio::test]
async fn statically_included_advisory_topic_is_forwarded_with_its_data_structure() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let remote_address = remote.address("openwire").unwrap();
    let advisory_topic = Destination::topic("ActiveMQ.Advisory.Queue");
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "advisories".into(),
            uris: vec![format!("tcp://{remote_address}")],
            static_included_destinations: vec![advisory_topic.to_uri()],
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let mut remote_client = connected(remote_address).await;
    remote_client
        .create_consumer(&advisory_topic, 100, None)
        .await
        .unwrap();
    wait_for_network_consumer(&local, &advisory_topic, true, "advisory bridge").await;

    let created = Destination::queue("advisory.forwarded.queue");
    let mut local_client = connected(local.address("openwire").unwrap()).await;
    send(&mut local_client, &created, "creates-queue").await;
    let local_id = local.core.info().id;
    let forwarded = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, message, _) = remote_client
                .receive_message(Duration::from_secs(5))
                .await
                .unwrap()
                .expect("forwarded advisory");
            if matches!(
                &message.advisory,
                Some(activemq_classic_rs::core::AdvisoryData::DestinationAdded(d)) if *d == created
            ) {
                break message;
            }
        }
    })
    .await
    .expect("destination advisory was not forwarded");
    assert_eq!(forwarded.destination, advisory_topic);
    assert_eq!(forwarded.broker_path, vec![local_id]);
    assert!(
        !remote
            .core
            .destinations()
            .iter()
            .any(|d| d.destination == created),
        "only the advisory, not the queue, crosses the bridge"
    );

    local.stop();
    remote.stop();
}

#[tokio::test]
async fn consumer_advisory_forwarding_and_dynamic_advisory_includes_are_rejected() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let remote_address = remote.address("openwire").unwrap();
    let mut local_config = broker_config();
    for (name, config) in [
        (
            "consumer-advisories",
            NetworkConnectorConfig {
                static_included_destinations: vec!["topic://ActiveMQ.Advisory.Consumer.>".into()],
                ..NetworkConnectorConfig::default()
            },
        ),
        (
            "dynamic-advisories",
            NetworkConnectorConfig {
                dynamically_included_destinations: vec!["topic://ActiveMQ.Advisory.Queue".into()],
                ..NetworkConnectorConfig::default()
            },
        ),
    ] {
        local_config
            .network_connectors
            .push(NetworkConnectorConfig {
                name: name.into(),
                uris: vec![format!("tcp://{remote_address}")],
                reconnect_delay_ms: 100,
                ..config
            });
    }
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        remote.core.connections().is_empty(),
        "rejected advisory connectors must not connect"
    );
    local.stop();
    remote.stop();
}

async fn request(
    client: &mut OpenWireClient,
    destination: &Destination,
    reply_to: &Destination,
    text: &str,
) {
    let producer = client.create_producer(Some(destination)).await.unwrap();
    let mut message = Message::text(destination.clone(), text);
    message.reply_to = Some(reply_to.clone());
    client.send(&producer, &message).await.unwrap();
}

async fn serve_one(service: &mut OpenWireClient, expected: &str, reply: &str) {
    let (consumer, message, _) = service
        .receive_message(Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("service did not receive {expected}"));
    assert_eq!(message.body, Body::Text(expected.into()));
    service
        .acknowledge(&consumer, &message.message_id)
        .await
        .unwrap();
    let reply_to = message.reply_to.expect("request carries reply_to");
    assert!(reply_to.is_temporary());
    send(service, &reply_to, reply).await;
}

fn has_destination(
    broker: &activemq_classic_rs::runtime::RunningBroker,
    destination: &Destination,
) -> bool {
    broker
        .core
        .destinations()
        .iter()
        .any(|entry| entry.destination == *destination)
}

async fn wait_destination_gone(
    broker: &activemq_classic_rs::runtime::RunningBroker,
    destination: &Destination,
    context: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while has_destination(broker, destination) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{context}: {destination} still exists"));
}

#[tokio::test]
async fn duplex_request_reply_uses_temporary_queue_proxies_in_both_directions() {
    let _guard = NETWORK_TEST_LOCK.lock().await;
    let remote = activemq_classic_rs::runtime::start(broker_config())
        .await
        .unwrap();
    let remote_address = remote.address("openwire").unwrap();
    let mut local_config = broker_config();
    local_config
        .network_connectors
        .push(NetworkConnectorConfig {
            name: "duplex-temp".into(),
            uris: vec![format!("tcp://{remote_address}")],
            duplex: true,
            reconnect_delay_ms: 100,
            ..NetworkConnectorConfig::default()
        });
    let local = activemq_classic_rs::runtime::start(local_config)
        .await
        .unwrap();
    let local_address = local.address("openwire").unwrap();
    let pid = std::process::id();

    // Local requester, remote service: the reply crosses the reverse
    // (duplex) direction into a locally owned temporary queue.
    let remote_service_queue = Destination::queue("rr.remote-service");
    let mut remote_service = connected(remote_address).await;
    remote_service
        .create_consumer(&remote_service_queue, 10, None)
        .await
        .unwrap();
    let mut local_requester = connected(local_address).await;
    let local_reply =
        create_temp_queue(&mut local_requester, &format!("ID:rr-local-{pid}:1:1")).await;
    let local_reply_consumer = local_requester
        .create_consumer(&local_reply, 10, None)
        .await
        .unwrap();
    wait_for_network_consumer(&remote, &local_reply, true, "remote proxy for local temp").await;
    wait_for_network_consumer(&local, &remote_service_queue, true, "remote service demand").await;
    assert!(has_destination(&remote, &local_reply));
    request(
        &mut local_requester,
        &remote_service_queue,
        &local_reply,
        "local-request",
    )
    .await;
    serve_one(&mut remote_service, "local-request", "remote-reply").await;
    let reply = expect_text(&mut local_requester, "remote-reply").await;
    assert_eq!(reply.broker_path, vec![remote.core.info().id]);

    // Remote requester, local service: the reply crosses the forward
    // direction into a remotely owned temporary queue.
    let local_service_queue = Destination::queue("rr.local-service");
    let mut local_service = connected(local_address).await;
    local_service
        .create_consumer(&local_service_queue, 10, None)
        .await
        .unwrap();
    let mut remote_requester = connected(remote_address).await;
    let remote_reply =
        create_temp_queue(&mut remote_requester, &format!("ID:rr-remote-{pid}:1:1")).await;
    remote_requester
        .create_consumer(&remote_reply, 10, None)
        .await
        .unwrap();
    wait_for_network_consumer(&local, &remote_reply, true, "local proxy for remote temp").await;
    wait_for_network_consumer(&remote, &local_service_queue, true, "local service demand").await;
    request(
        &mut remote_requester,
        &local_service_queue,
        &remote_reply,
        "remote-request",
    )
    .await;
    serve_one(&mut local_service, "remote-request", "local-reply").await;
    let reply = expect_text(&mut remote_requester, "local-reply").await;
    assert_eq!(reply.broker_path, vec![local.core.info().id]);
    // A local proxy of a remote-owned temporary must never be mirrored back.
    assert!(
        !remote
            .core
            .consumers()
            .iter()
            .any(|consumer| consumer.network && consumer.destination == remote_reply.to_uri()),
        "remote-owned temporary queue was echoed back as duplex demand"
    );
    expect_silence(&mut local_requester, "local requester duplicate").await;
    expect_silence(&mut remote_requester, "remote requester duplicate").await;

    // Owner lifecycle: deleting the local temporary removes the remote proxy;
    // dropping the remote owner removes the local proxy.
    local_requester
        .remove_consumer(&local_reply_consumer)
        .await
        .unwrap();
    local_requester
        .remove_destination(&local_reply)
        .await
        .unwrap();
    wait_destination_gone(
        &remote,
        &local_reply,
        "remote proxy after local temp deletion",
    )
    .await;
    wait_for_network_consumer(&remote, &local_reply, false, "remote proxy consumer").await;
    drop(remote_requester);
    wait_destination_gone(
        &local,
        &remote_reply,
        "local proxy after remote owner closed",
    )
    .await;
    assert!(has_destination(&local, &local_service_queue));

    local.stop();
    remote.stop();
}
