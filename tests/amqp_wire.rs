use std::io;

use activemq_classic_rs::config::AppConfig;
use activemq_classic_rs::core::security::UserConfig;
use activemq_classic_rs::runtime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn string(value: &str) -> Vec<u8> {
    let mut out = vec![0xa1, value.len() as u8];
    out.extend_from_slice(value.as_bytes());
    out
}

fn symbol(value: &str) -> Vec<u8> {
    let mut out = vec![0xa3, value.len() as u8];
    out.extend_from_slice(value.as_bytes());
    out
}

fn binary(value: &[u8]) -> Vec<u8> {
    let mut out = vec![0xa0, value.len() as u8];
    out.extend_from_slice(value);
    out
}

fn uint(value: u32) -> Vec<u8> {
    let mut out = vec![0x70];
    out.extend_from_slice(&value.to_be_bytes());
    out
}

fn described(descriptor: u8, fields: Vec<Vec<u8>>) -> Vec<u8> {
    let mut body = vec![0x00, 0x53, descriptor, 0xc0];
    let mut contents = vec![fields.len() as u8];
    for field in fields {
        contents.extend(field);
    }
    body.push(contents.len() as u8);
    body.extend(contents);
    body
}

fn frame(payload: &[u8]) -> Vec<u8> {
    frame_type(payload, 0)
}

fn frame_type(payload: &[u8], frame_type: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 8);
    out.extend_from_slice(&((payload.len() + 8) as u32).to_be_bytes());
    out.extend_from_slice(&[2, frame_type, 0, 0]);
    out.extend_from_slice(payload);
    out
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut header = [0; 8];
    stream.read_exact(&mut header).await?;
    let size = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
    let mut data = vec![0; size - 8];
    stream.read_exact(&mut data).await?;
    Ok(data)
}

fn amqp_config() -> AppConfig {
    let mut config = AppConfig::default();
    config.broker.name = "amqp-wire-test".into();
    config.persistence.backend = "memory".into();
    config.web_console.enabled = false;
    config.protocols.openwire.enabled = false;
    config.protocols.stomp.enabled = false;
    config.protocols.mqtt.enabled = false;
    config.protocols.amqp.enabled = true;
    config.protocols.amqp.bind = "127.0.0.1:0".into();
    config
}

#[tokio::test]
async fn transaction_coordinator_uses_dispositions_for_symbolic_commands() {
    let broker = runtime::start(amqp_config()).await.unwrap();
    let mut client = TcpStream::connect(broker.address("amqp").unwrap())
        .await
        .unwrap();
    client.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut header = [0; 8];
    client.read_exact(&mut header).await.unwrap();
    client
        .write_all(&frame(&described(0x10, vec![string("tx-wire-client")])))
        .await
        .unwrap();
    read_frame(&mut client).await.unwrap();
    client
        .write_all(&frame(&described(
            0x11,
            vec![vec![0x40], uint(0), uint(100), uint(100)],
        )))
        .await
        .unwrap();
    let begin = read_frame(&mut client).await.unwrap();
    assert!(
        begin.windows(3).any(|bytes| bytes == [0x60, 0, 0]),
        "remote-channel must be encoded as ushort: {begin:?}"
    );
    client
        .write_all(&frame(&described(
            0x12,
            vec![
                string("coordinator"),
                uint(0),
                vec![0x42],
                vec![0x50, 2],
                vec![0x50, 0],
                vec![0x40],
                described(0x30, vec![]),
                vec![0x40],
                vec![0x40],
                uint(0),
            ],
        )))
        .await
        .unwrap();
    read_frame(&mut client).await.unwrap();
    read_frame(&mut client).await.unwrap();
    for (delivery_id, rollback) in [(0, false), (2, true)] {
        let mut transfer = described(
            0x14,
            vec![uint(0), uint(delivery_id), binary(b"declare"), uint(0)],
        );
        transfer.extend_from_slice(&[0x00, 0x53, 0x77, 0x00]);
        transfer.extend(symbol("amqp:declare:list"));
        transfer.push(0x45);
        client.write_all(&frame(&transfer)).await.unwrap();
        let declared = read_frame(&mut client).await.unwrap();
        assert_eq!(&declared[..3], &[0x00, 0x53, 0x15]);
        assert!(
            declared.windows(3).any(|bytes| bytes == [0x00, 0x53, 0x33]),
            "expected declared outcome: {declared:?}"
        );
        let mut discharge = vec![0x00];
        discharge.extend(symbol("amqp:discharge:list"));
        let mut fields = binary(&delivery_id.to_be_bytes());
        fields.push(if rollback { 0x41 } else { 0x42 });
        discharge.extend([0xc0, (fields.len() + 1) as u8, 2]);
        discharge.extend(fields);
        let mut transfer = described(
            0x14,
            vec![
                uint(0),
                uint(delivery_id + 1),
                binary(b"discharge"),
                uint(0),
            ],
        );
        transfer.extend([0x00, 0x53, 0x77]);
        transfer.extend(discharge);
        client.write_all(&frame(&transfer)).await.unwrap();
        let accepted = read_frame(&mut client).await.unwrap();
        assert_eq!(&accepted[..3], &[0x00, 0x53, 0x15]);
        assert!(
            accepted.windows(3).any(|bytes| bytes == [0x00, 0x53, 0x24]),
            "expected accepted outcome: {accepted:?}"
        );
    }
    drop(client);
    broker.stop();
}

async fn sasl_exchange(mechanism: &str, auth: &[u8], config: AppConfig, accepted: bool) {
    let broker = runtime::start(config).await.unwrap();
    let mut client = TcpStream::connect(broker.address("amqp").unwrap())
        .await
        .unwrap();
    client.write_all(b"AMQP\x03\x01\0\0").await.unwrap();
    let mut header = [0; 8];
    client.read_exact(&mut header).await.unwrap();
    assert_eq!(&header, b"AMQP\x03\x01\0\0");
    let mechanisms = read_frame(&mut client).await.unwrap();
    assert_eq!(&mechanisms[..3], &[0x00, 0x53, 0x40]);

    let init = described(0x41, vec![symbol(mechanism), binary(auth)]);
    client.write_all(&frame_type(&init, 1)).await.unwrap();
    let outcome = read_frame(&mut client).await.unwrap();
    assert_eq!(&outcome[..3], &[0x00, 0x53, 0x44]);
    if accepted {
        client.write_all(b"AMQP\0\x01\0\0").await.unwrap();
        client.read_exact(&mut header).await.unwrap();
        assert_eq!(&header, b"AMQP\0\x01\0\0");
    } else {
        assert!(
            outcome.ends_with(&[0x50, 1]),
            "unexpected SASL outcome: {outcome:?}"
        );
    }
    drop(client);
    broker.stop();
}

#[tokio::test]
async fn accepts_amqp_header_open_and_close_without_sasl() {
    let broker = runtime::start(amqp_config()).await.unwrap();
    let address = broker.address("amqp").unwrap();
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut header = [0; 8];
    client.read_exact(&mut header).await.unwrap();
    assert_eq!(&header, b"AMQP\0\x01\0\0");

    let open = described(
        0x10,
        vec![
            string("raw-amqp-test"),
            vec![0x40],
            uint(32 * 1024),
            uint(u16::MAX as u32),
        ],
    );
    client.write_all(&frame(&open)).await.unwrap();
    let response = read_frame(&mut client).await.unwrap();
    assert_eq!(&response[..3], &[0x00, 0x53, 0x10]);
    assert_eq!(response[3], 0xc0);

    let close = described(0x18, vec![vec![0x40]]);
    client.write_all(&frame(&close)).await.unwrap();
    let _ = read_frame(&mut client).await;
    drop(client);
    broker.stop();
}

#[tokio::test]
async fn negotiates_sasl_anonymous_when_security_is_disabled() {
    sasl_exchange("ANONYMOUS", b"anonymous", amqp_config(), true).await;
}

#[tokio::test]
async fn negotiates_sasl_plain_and_rejects_bad_credentials() {
    let mut secure = amqp_config();
    secure.security.enabled = true;
    secure.security.users.push(UserConfig {
        username: "guest".into(),
        password: Some("guest".into()),
        ..Default::default()
    });
    sasl_exchange("PLAIN", b"\0guest\0guest", secure.clone(), true).await;
    sasl_exchange("PLAIN", b"\0guest\0wrong", secure, false).await;
}
