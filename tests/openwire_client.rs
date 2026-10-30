use std::time::Duration;

use activemq_classic_rs::config::{AppConfig, ListenerConfig};
use activemq_classic_rs::core::{Body, Destination, DestinationKind, Message};
use activemq_classic_rs::protocols::openwire::OpenWireClient;

fn test_config() -> AppConfig {
    let mut config = AppConfig::default();
    config.protocols.openwire = ListenerConfig::new(true, "127.0.0.1:0");
    config.protocols.stomp.enabled = false;
    config.protocols.amqp.enabled = false;
    config.protocols.mqtt.enabled = false;
    config.persistence.backend = "memory".to_owned();
    config.web_console.enabled = false;
    config
}

async fn wait_for_connection_cleanup(core: &activemq_classic_rs::core::BrokerCore) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if core.connections().is_empty() && core.consumers().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("OpenWire connection and consumers are cleaned up");
}

#[tokio::test]
async fn public_client_round_trips_queue_messages_and_acknowledgements() {
    let broker = activemq_classic_rs::runtime::start(test_config())
        .await
        .unwrap();
    let mut client = OpenWireClient::connect_client(
        broker.address("openwire").unwrap(),
        8 * 1024 * 1024,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let destination = Destination::new(DestinationKind::Queue, "openwire-client-test");
    let consumer = client
        .create_consumer(&destination, 10, None)
        .await
        .unwrap();
    let producer = client.create_producer(Some(&destination)).await.unwrap();
    let message =
        Message::text(destination, "client API round trip").with_property("sequence", 42_i32);
    let message_id = message.message_id.clone();
    client.send(&producer, &message).await.unwrap();

    let (_, received, redelivery) = client
        .receive_message(Duration::from_secs(3))
        .await
        .unwrap()
        .expect("message delivery");
    assert_eq!(received.message_id, message_id);
    assert_eq!(
        received.body,
        Body::Text("client API round trip".to_owned())
    );
    assert_eq!(received.properties.get("sequence"), Some(&42_i32.into()));
    assert_eq!(redelivery, 0);
    client.acknowledge(&consumer, &message_id).await.unwrap();
    broker.stop();
}

#[tokio::test]
async fn public_client_receives_authentication_and_authorization_failures() {
    use activemq_classic_rs::core::security::{AuthorizationEntry, UserConfig};

    let mut config = test_config();
    config.security.enabled = true;
    config.security.users.push(UserConfig {
        username: "wire-user".to_owned(),
        password: Some("correct".to_owned()),
        groups: vec!["wire-users".to_owned()],
        ..UserConfig::default()
    });
    config.security.authorization.push(AuthorizationEntry {
        destination: "queue://allowed".to_owned(),
        read: vec!["wire-users".to_owned()],
        write: vec!["wire-users".to_owned()],
        ..AuthorizationEntry::default()
    });
    let broker = activemq_classic_rs::runtime::start(config).await.unwrap();
    let address = broker.address("openwire").unwrap();
    let bad_credentials = OpenWireClient::connect_client(
        address,
        8 * 1024 * 1024,
        None,
        Some("wire-user"),
        Some("wrong"),
    )
    .await;
    assert!(bad_credentials.is_err());

    let mut client = OpenWireClient::connect_client(
        address,
        8 * 1024 * 1024,
        None,
        Some("wire-user"),
        Some("correct"),
    )
    .await
    .unwrap();
    let denied = Destination::new(DestinationKind::Queue, "forbidden");
    assert!(client.create_consumer(&denied, 10, None).await.is_err());
    broker.stop();
}

#[tokio::test]
async fn abrupt_openwire_close_releases_connection_and_consumers() {
    let broker = activemq_classic_rs::runtime::start(test_config())
        .await
        .unwrap();
    let mut client = OpenWireClient::connect_client(
        broker.address("openwire").unwrap(),
        8 * 1024 * 1024,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    client
        .create_consumer(
            &Destination::new(DestinationKind::Queue, "cleanup-abrupt"),
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(broker.core.connections().len(), 1);
    assert_eq!(broker.core.consumers().len(), 1);
    drop(client);
    wait_for_connection_cleanup(&broker.core).await;
    broker.stop();
}

#[tokio::test]
async fn malformed_openwire_command_releases_established_connection() {
    use tokio::io::AsyncWriteExt;

    let broker = activemq_classic_rs::runtime::start(test_config())
        .await
        .unwrap();
    let client = OpenWireClient::connect_client(
        broker.address("openwire").unwrap(),
        8 * 1024 * 1024,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let (_reader, mut stream) = client.into_parts();
    assert_eq!(broker.core.connections().len(), 1);
    stream.write_all(&[0, 0, 0, 1, 5]).await.unwrap();
    wait_for_connection_cleanup(&broker.core).await;
    broker.stop();
}

#[tokio::test]
async fn public_client_manages_temporary_destinations_and_network_consumers() {
    let broker = activemq_classic_rs::runtime::start(test_config())
        .await
        .unwrap();
    let mut client = OpenWireClient::connect_client(
        broker.address("openwire").unwrap(),
        8 * 1024 * 1024,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let proxy_destination = Destination::new(DestinationKind::TempQueue, "proxy-temp-destination");
    client.add_destination(&proxy_destination).await.unwrap();
    assert!(
        broker
            .core
            .destinations()
            .iter()
            .any(|item| item.destination == proxy_destination)
    );
    client.remove_destination(&proxy_destination).await.unwrap();
    let destination = client
        .add_temporary_destination(DestinationKind::TempQueue)
        .await
        .unwrap();
    let consumer = client
        .create_network_consumer(
            &destination,
            10,
            Some("priority >= 0"),
            &["upstream".into()],
        )
        .await
        .unwrap();
    assert_eq!(broker.core.consumers().len(), 1);
    client.remove_consumer(&consumer).await.unwrap();
    assert!(broker.core.consumers().is_empty());
    client.remove_destination(&destination).await.unwrap();
    assert!(
        broker
            .core
            .destinations()
            .iter()
            .all(|item| item.destination != destination)
    );
    broker.stop();
}

#[tokio::test]
async fn public_client_uses_unique_ids_and_exposes_remote_broker_identity() {
    let broker = activemq_classic_rs::runtime::start(test_config())
        .await
        .unwrap();
    let address = broker.address("openwire").unwrap();
    let mut first = OpenWireClient::connect_client(address, 8 * 1024 * 1024, None, None, None)
        .await
        .unwrap();
    let mut second = OpenWireClient::connect_client(address, 8 * 1024 * 1024, None, None, None)
        .await
        .unwrap();
    assert!(
        first
            .remote_broker_id()
            .is_some_and(|id| id.starts_with("ID:activemq-classic-rs-"))
    );
    let destination = Destination::new(DestinationKind::Queue, "multiple-openwire-clients");
    let first_id = first.create_consumer(&destination, 1, None).await.unwrap();
    let second_id = second.create_consumer(&destination, 1, None).await.unwrap();
    assert_ne!(first_id, second_id);
    broker.stop();
}
