use std::time::Duration;

use activemq_classic_rs::config::{AppConfig, ListenerConfig};
use activemq_classic_rs::core::{Body, Destination, DestinationKind, Message};
use activemq_classic_rs::protocols::openwire::OpenWireClient;

#[tokio::test]
async fn public_client_round_trips_queue_messages_and_acknowledgements() {
    let mut config = AppConfig::default();
    config.protocols.openwire = ListenerConfig::new(true, "127.0.0.1:0");
    config.protocols.stomp.enabled = false;
    config.protocols.amqp.enabled = false;
    config.protocols.mqtt.enabled = false;
    config.persistence.backend = "memory".to_owned();
    config.web_console.enabled = false;
    let broker = activemq_classic_rs::runtime::start(config).await.unwrap();
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

    let mut config = AppConfig::default();
    config.protocols.openwire = ListenerConfig::new(true, "127.0.0.1:0");
    config.protocols.stomp.enabled = false;
    config.protocols.amqp.enabled = false;
    config.protocols.mqtt.enabled = false;
    config.persistence.backend = "memory".to_owned();
    config.web_console.enabled = false;
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
