use std::time::Duration;

use activemq_classic_rs::config::ListenerConfig;
use activemq_classic_rs::core::security::SecurityManager;
use activemq_classic_rs::core::{Body, BrokerCore, Destination, Policy, Value};
use activemq_classic_rs::protocols::{self, ProtocolContext};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

async fn frame(stream: &mut TcpStream) -> String {
    timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        loop {
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            if byte[0] == 0 {
                return String::from_utf8_lossy(&bytes).into_owned();
            }
            bytes.push(byte[0]);
        }
    })
    .await
    .expect("STOMP frame timeout")
}

#[tokio::test]
async fn stomp_map_json_creates_native_map_and_respects_content_length() {
    let core = BrokerCore::in_memory();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let context = ProtocolContext::new(
        core.clone(),
        ListenerConfig::new(true, "127.0.0.1:0"),
        1024 * 1024,
    );
    let task = tokio::spawn(protocols::stomp::serve(context, listener));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"STOMP\naccept-version:1.2\nhost:localhost\n\n\0")
        .await
        .unwrap();
    assert!(frame(&mut stream).await.starts_with("CONNECTED\n"));
    let json = r#"{"map":{"entry":[{"string":["name","order"]},{"string":"count","int":42}]}}"#;
    stream
        .write_all(
            format!("SEND\ndestination:/queue/maps\ntransformation:jms-map-json\nreceipt:map\n\n{json}\0")
                .as_bytes(),
        )
        .await
        .unwrap();
    assert!(frame(&mut stream).await.starts_with("RECEIPT\n"));
    let destination = Destination::queue("maps");
    let pending = core.browse(&destination, 10).unwrap();
    let message = core
        .get_message(&destination, &pending[0].message_id)
        .unwrap();
    let Body::Map(map) = &message.body else {
        panic!("expected native map, got {:?}", message.body);
    };
    assert_eq!(map["name"], Value::String("order".into()));
    assert_eq!(map["count"], Value::Int(42));
    stream
        .write_all(
            format!("SEND\ndestination:/queue/bytes\ntransformation:jms-map-json\ncontent-length:{}\nreceipt:bytes\n\n{json}\0", json.len())
                .as_bytes(),
        )
        .await
        .unwrap();
    assert!(frame(&mut stream).await.starts_with("RECEIPT\n"));
    let destination = Destination::queue("bytes");
    let pending = core.browse(&destination, 10).unwrap();
    assert_eq!(
        core.get_message(&destination, &pending[0].message_id)
            .unwrap()
            .body,
        Body::Bytes(json.as_bytes().to_vec())
    );
    stream
        .write_all(
            b"SEND\ndestination:/queue/invalid\ntransformation:jms-map-json\nreceipt:bad\n\n[]\0",
        )
        .await
        .unwrap();
    let error = frame(&mut stream).await;
    assert!(error.starts_with("ERROR\n"), "{error}");
    assert!(error.contains("receipt-id:bad"), "{error}");
    assert!(core.browse(&Destination::queue("invalid"), 10).is_err());
    task.abort();
}

#[tokio::test]
async fn stomp_12_queue_receipt_and_transaction() {
    let core = BrokerCore::open(
        "stomp-test",
        None,
        Policy {
            advisory_support: false,
            ..Policy::default()
        },
        SecurityManager::disabled(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let context = ProtocolContext::new(core, ListenerConfig::new(true, "127.0.0.1:0"), 1024 * 1024);
    let task = tokio::spawn(protocols::stomp::serve(context, listener));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"STOMP\naccept-version:1.0,1.1,1.2\nhost:localhost\nheart-beat:0,0\n\n\0")
        .await
        .unwrap();
    assert!(frame(&mut stream).await.starts_with("CONNECTED\n"));
    stream.write_all(b"SUBSCRIBE\nid:sub-1\ndestination:/queue/stomp-test\nack:auto\nreceipt:subscribed\n\n\0").await.unwrap();
    assert!(
        frame(&mut stream)
            .await
            .starts_with("RECEIPT\nreceipt-id:subscribed")
    );
    stream.write_all(b"BEGIN\ntransaction:tx-1\n\n\0SEND\ndestination:/queue/stomp-test\ntransaction:tx-1\nreceipt:sent\n\nhello\0COMMIT\ntransaction:tx-1\nreceipt:committed\n\n\0").await.unwrap();
    let received = frame(&mut stream).await;
    assert!(
        received.starts_with("RECEIPT\nreceipt-id:sent") || received.starts_with("MESSAGE\n"),
        "{received}"
    );
    let second = frame(&mut stream).await;
    if received.starts_with("MESSAGE\n") {
        assert!(
            second.starts_with("RECEIPT\nreceipt-id:sent")
                || second.starts_with("RECEIPT\nreceipt-id:committed"),
            "{second}"
        );
    }
    let message = if received.starts_with("MESSAGE\n") {
        received
    } else if second.starts_with("MESSAGE\n") {
        second
    } else {
        frame(&mut stream).await
    };
    assert!(message.starts_with("MESSAGE\n"), "{message}");
    assert!(message.ends_with("\n\nhello"), "{message:?}");
    stream
        .write_all(b"DISCONNECT\nreceipt:bye\n\n\0")
        .await
        .unwrap();
    assert!(
        frame(&mut stream)
            .await
            .starts_with("RECEIPT\nreceipt-id:bye")
    );
    drop(stream);
    task.abort();
}

#[tokio::test]
async fn stomp_invalid_command_returns_error() {
    let core = BrokerCore::open(
        "stomp-test",
        None,
        Policy::default(),
        SecurityManager::disabled(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let context = ProtocolContext::new(core, ListenerConfig::new(true, "127.0.0.1:0"), 128);
    let task = tokio::spawn(protocols::stomp::serve(context, listener));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"CONNECT\naccept-version:1.2\nhost:localhost\n\n\0UNKNOWN\n\n\0")
        .await
        .unwrap();
    assert!(frame(&mut stream).await.starts_with("CONNECTED\n"));
    assert!(frame(&mut stream).await.starts_with("ERROR\n"));
    drop(stream);
    task.abort();
}

#[tokio::test]
async fn stomp_composite_destinations_route_to_each_queue() {
    let core = BrokerCore::open(
        "stomp-composite-test",
        None,
        Policy {
            advisory_support: false,
            ..Policy::default()
        },
        SecurityManager::disabled(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let context = ProtocolContext::new(core, ListenerConfig::new(true, "127.0.0.1:0"), 1024);
    let task = tokio::spawn(protocols::stomp::serve(context, listener));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"CONNECT\naccept-version:1.1\nhost:localhost\n\n\0")
        .await
        .unwrap();
    assert!(
        frame(&mut stream)
            .await
            .starts_with("CONNECTED\nversion:1.1")
    );
    stream
        .write_all(
            b"SUBSCRIBE\nid:composite\ndestination:/queue/fanout-a,/queue/fanout-b\nack:auto\n\n\0",
        )
        .await
        .unwrap();
    stream
        .write_all(b"SEND\ndestination:/queue/fanout-a,/queue/fanout-b\n\nfanout\0")
        .await
        .unwrap();
    let first = frame(&mut stream).await;
    let second = frame(&mut stream).await;
    assert!(
        first.starts_with("MESSAGE\n") && first.ends_with("\n\nfanout"),
        "{first:?}"
    );
    assert!(
        second.starts_with("MESSAGE\n") && second.ends_with("\n\nfanout"),
        "{second:?}"
    );
    drop(stream);
    task.abort();
}

#[tokio::test]
async fn stomp_10_uses_unescaped_headers_and_no_host() {
    let core = BrokerCore::open(
        "stomp-10-test",
        None,
        Policy::default(),
        SecurityManager::disabled(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let context = ProtocolContext::new(core, ListenerConfig::new(true, "127.0.0.1:0"), 1024);
    let task = tokio::spawn(protocols::stomp::serve(context, listener));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(b"CONNECT\n\n\0").await.unwrap();
    assert!(frame(&mut stream).await.starts_with("CONNECTED\n"));
    stream.write_all(b"SUBSCRIBE\ndestination:/queue/stomp10\n\n\0SEND\ndestination:/queue/stomp10\n\nlegacy\0").await.unwrap();
    let received = frame(&mut stream).await;
    assert!(
        received.starts_with("MESSAGE\n") && received.ends_with("\n\nlegacy"),
        "{received:?}"
    );
    drop(stream);
    task.abort();
}
