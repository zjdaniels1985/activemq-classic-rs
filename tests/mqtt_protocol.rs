use activemq_classic_rs::config::AppConfig;
use activemq_classic_rs::core::{Destination, Message};
use activemq_classic_rs::runtime;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

fn config() -> AppConfig {
    let mut c = AppConfig::default();
    c.persistence.backend = "memory".into();
    c.protocols.openwire.enabled = false;
    c.protocols.stomp.enabled = false;
    c.protocols.amqp.enabled = false;
    c.protocols.mqtt.bind = "127.0.0.1:0".into();
    c.web_console.enabled = false;
    c.policy.advisory_support = false;
    c
}
fn text(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(&(s.len() as u16).to_be_bytes());
    b.extend_from_slice(s.as_bytes());
}
fn connect_body(version: u8, id: &str, flags: u8, keep: u16) -> Vec<u8> {
    let mut b = Vec::new();
    text(&mut b, if version == 3 { "MQIsdp" } else { "MQTT" });
    b.extend_from_slice(&[version, flags]);
    b.extend_from_slice(&keep.to_be_bytes());
    text(&mut b, id);
    b
}
struct Client(TcpStream);
impl Client {
    async fn raw(a: SocketAddr) -> Self {
        Self(TcpStream::connect(a).await.unwrap())
    }
    async fn connect(a: SocketAddr, id: &str, clean: bool) -> (Self, bool) {
        let mut c = Self::raw(a).await;
        c.send(0x10, &connect_body(4, id, u8::from(clean) * 2, 0))
            .await;
        let (h, b) = c.read().await;
        assert_eq!(h, 0x20);
        assert_eq!(b[1], 0);
        (c, b[0] != 0)
    }
    async fn send(&mut self, h: u8, b: &[u8]) {
        let mut out = vec![h];
        let mut n = b.len();
        loop {
            let digit = (n % 128) as u8;
            n /= 128;
            out.push(digit | if n > 0 { 128 } else { 0 });
            if n == 0 {
                break;
            }
        }
        out.extend_from_slice(b);
        self.0.write_all(&out).await.unwrap();
    }
    async fn read(&mut self) -> (u8, Vec<u8>) {
        timeout(Duration::from_secs(3), async {
            let h = self.0.read_u8().await.unwrap();
            let mut n = 0;
            let mut shift = 0;
            loop {
                let digit = self.0.read_u8().await.unwrap();
                n |= usize::from(digit & 127) << shift;
                shift += 7;
                if digit & 128 == 0 {
                    break;
                }
            }
            let mut b = vec![0; n];
            self.0.read_exact(&mut b).await.unwrap();
            (h, b)
        })
        .await
        .expect("MQTT response timeout")
    }
    async fn subscribe(&mut self, topic: &str, qos: u8) {
        let mut b = vec![0, 1];
        text(&mut b, topic);
        b.push(qos);
        self.send(0x82, &b).await;
        assert_eq!(self.read().await, (0x90, vec![0, 1, qos]));
    }
    async fn publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool, id: u16) {
        let mut b = Vec::new();
        text(&mut b, topic);
        if qos > 0 {
            b.extend_from_slice(&id.to_be_bytes());
        }
        b.extend_from_slice(payload);
        self.send(0x30 | qos << 1 | u8::from(retain), &b).await;
    }
    async fn received(&mut self) -> (u8, String, Vec<u8>, u16) {
        let (h, b) = self.read().await;
        assert_eq!(h >> 4, 3);
        let n = usize::from(u16::from_be_bytes([b[0], b[1]]));
        let topic = String::from_utf8(b[2..2 + n].to_vec()).unwrap();
        let start = 2 + n;
        let qos = (h >> 1) & 3;
        let id = if qos == 0 {
            0
        } else {
            u16::from_be_bytes([b[start], b[start + 1]])
        };
        (
            h,
            topic,
            b[start + if qos == 0 { 0 } else { 2 }..].to_vec(),
            id,
        )
    }
    async fn closed(&mut self) {
        let mut b = [0];
        let result = timeout(Duration::from_secs(3), self.0.read(&mut b))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0) | Err(_)), "not closed: {result:?}");
    }
    async fn disconnect(&mut self) {
        self.send(0xe0, &[]).await;
        self.closed().await;
    }
}

#[tokio::test]
async fn connect_versions_auth_and_validation() {
    let broker = runtime::start(config()).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    for v in [3, 4] {
        let mut c = Client::raw(a).await;
        c.send(0x10, &connect_body(v, "", 2, 0)).await;
        assert_eq!(c.read().await, (0x20, vec![0, 0]));
        c.send(0xc0, &[]).await;
        assert_eq!(c.read().await, (0xd0, vec![]));
        c.disconnect().await;
    }
    for (v, id, flags, code) in [(5, "v5", 2, 1), (4, "", 0, 2)] {
        let mut c = Client::raw(a).await;
        c.send(0x10, &connect_body(v, id, flags, 0)).await;
        assert_eq!(c.read().await, (0x20, vec![0, code]));
        c.closed().await;
    }
    let mut c = Client::raw(a).await;
    c.send(0x10, &connect_body(4, "bad", 3, 0)).await;
    c.closed().await;
    let (mut c, _) = Client::connect(a, "filters", true).await;
    let mut b = vec![0, 2];
    text(&mut b, "a/#/bad");
    b.push(1);
    c.send(0x82, &b).await;
    assert_eq!(c.read().await, (0x90, vec![0, 2, 128]));
    c.disconnect().await;
    broker.stop();
    let mut cfg = config();
    cfg.security.enabled = true;
    cfg.security
        .users
        .push(activemq_classic_rs::core::security::UserConfig {
            username: "user".into(),
            password: Some("pass".into()),
            ..Default::default()
        });
    let broker = runtime::start(cfg).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    for (password, code) in [("wrong", 4), ("pass", 0)] {
        let mut c = Client::raw(a).await;
        let mut b = connect_body(4, "auth", 0xc2, 0);
        text(&mut b, "user");
        text(&mut b, password);
        c.send(0x10, &b).await;
        assert_eq!(c.read().await, (0x20, vec![0, code]));
    }
    let mut c = Client::raw(a).await;
    c.send(0x10, &connect_body(4, "anon", 2, 0)).await;
    assert_eq!(c.read().await, (0x20, vec![0, 5]));
    broker.stop();
}

#[tokio::test]
async fn qos_zero_one_two_and_deduplication() {
    let broker = runtime::start(config()).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let (mut s, _) = Client::connect(a, "sub", true).await;
    let (mut p, _) = Client::connect(a, "pub", true).await;
    s.subscribe("a/+/c/#", 2).await;
    for qos in 0..=2 {
        p.publish("a/b/c/d", b"message", qos, false, 17).await;
        if qos == 1 {
            assert_eq!(p.read().await, (0x40, vec![0, 17]));
        }
        if qos == 2 {
            assert_eq!(p.read().await, (0x50, vec![0, 17]));
            p.publish("a/b/c/d", b"message", 2, false, 17).await;
            assert_eq!(p.read().await, (0x50, vec![0, 17]));
            for _ in 0..2 {
                p.send(0x62, &[0, 17]).await;
                assert_eq!(p.read().await, (0x70, vec![0, 17]));
            }
        }
        let (h, t, b, id) = s.received().await;
        assert_eq!(h, 0x30 | qos << 1);
        assert_eq!(t, "a/b/c/d");
        assert_eq!(b, b"message");
        if qos == 1 {
            s.send(0x40, &id.to_be_bytes()).await;
        }
        if qos == 2 {
            for _ in 0..2 {
                s.send(0x50, &id.to_be_bytes()).await;
                assert_eq!(s.read().await, (0x62, id.to_be_bytes().to_vec()));
            }
            s.send(0x70, &id.to_be_bytes()).await;
        }
    }
    s.send(0xc0, &[]).await;
    assert_eq!(s.read().await, (0xd0, vec![]));
    s.disconnect().await;
    p.disconnect().await;
    broker.stop();
}

#[tokio::test]
async fn repeated_subscribe_replays_retained_without_replacing_inflight_packets() {
    let broker = runtime::start(config()).await.unwrap();
    let address = broker.address("mqtt").unwrap();
    let (mut publisher, _) = Client::connect(address, "replay-publisher", true).await;
    for (id, topic) in [(1, "replay/a"), (2, "replay/b")] {
        publisher.publish(topic, b"saved", 1, true, id).await;
        assert_eq!(publisher.read().await.0, 0x40);
    }
    let (mut subscriber, _) = Client::connect(address, "replay-durable", false).await;
    subscriber.subscribe("replay/#", 1).await;
    let mut original_ids = Vec::new();
    for _ in 0..2 {
        let (header, _, body, id) = subscriber.received().await;
        assert_eq!(header, 0x33);
        assert_eq!(body, b"saved");
        original_ids.push(id);
    }
    let consumer = broker.core.consumers()[0].id;
    for _ in 0..2 {
        subscriber.subscribe("replay/#", 1).await;
        for _ in 0..2 {
            let (header, _, body, id) = subscriber.received().await;
            assert_eq!(header, 0x33, "a new replay must not have DUP set");
            assert_eq!(body, b"saved");
            assert!(
                !original_ids.contains(&id),
                "replay reused an in-flight packet ID"
            );
            subscriber.send(0x40, &id.to_be_bytes()).await;
        }
        assert_eq!(broker.core.consumers()[0].id, consumer);
        assert_eq!(broker.core.durable_subscriptions().len(), 1);
    }
    for id in original_ids {
        subscriber.send(0x40, &id.to_be_bytes()).await;
    }
    subscriber.send(0xc0, &[]).await;
    assert_eq!(subscriber.read().await, (0xd0, vec![]));
    subscriber.disconnect().await;
    publisher.disconnect().await;
    broker.stop();
}

#[tokio::test]
async fn retained_clear_will_and_cross_protocol_mapping() {
    let broker = runtime::start(config()).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let (mut p, _) = Client::connect(a, "retain", true).await;
    p.publish("a.b/c", b"saved", 1, true, 1).await;
    assert_eq!(p.read().await.0, 0x40);
    let (mut s, _) = Client::connect(a, "sub", true).await;
    s.subscribe("a.b/c", 1).await;
    let (h, t, b, id) = s.received().await;
    assert_eq!(
        (h, t.as_str(), b.as_slice()),
        (0x33, "a.b/c", &b"saved"[..])
    );
    s.send(0x40, &id.to_be_bytes()).await;
    s.disconnect().await;
    p.publish("a.b/c", b"", 1, true, 2).await;
    assert_eq!(p.read().await.0, 0x40);
    let (mut s, _) = Client::connect(a, "clear", true).await;
    s.subscribe("a.b/c", 1).await;
    s.send(0xc0, &[]).await;
    assert_eq!(s.read().await, (0xd0, vec![]));
    s.subscribe("last/will", 2).await;
    let mut w = Client::raw(a).await;
    let mut b = connect_body(4, "will", 0x2e, 0);
    text(&mut b, "last/will");
    text(&mut b, "gone");
    w.send(0x10, &b).await;
    assert_eq!(w.read().await, (0x20, vec![0, 0]));
    drop(w);
    let (h, _, b, id) = s.received().await;
    assert_eq!(h, 0x32);
    assert_eq!(b, b"gone");
    s.send(0x40, &id.to_be_bytes()).await;
    s.subscribe("a/b", 0).await;
    let (internal, _) = broker.core.connect_internal("test");
    broker
        .core
        .send(
            internal,
            Message::text(Destination::topic("a.b"), "from JMS"),
            None,
        )
        .unwrap();
    let (h, t, b, _) = s.received().await;
    assert_eq!(
        (h, t.as_str(), b.as_slice()),
        (0x30, "a/b", &b"from JMS"[..])
    );
    broker.core.disconnect(internal);
    s.disconnect().await;
    p.disconnect().await;
    broker.stop();
}

#[tokio::test]
async fn persistent_offline_delivery_redelivery_and_clean_reset() {
    let broker = runtime::start(config()).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let (mut s, present) = Client::connect(a, "durable", false).await;
    assert!(!present);
    s.subscribe("persistent", 2).await;
    s.disconnect().await;
    let (mut p, _) = Client::connect(a, "publisher", true).await;
    p.publish("persistent", b"offline", 1, false, 1).await;
    p.read().await;
    let (mut s, present) = Client::connect(a, "durable", false).await;
    assert!(present);
    let (h, _, b, id) = s.received().await;
    assert_eq!(h, 0x32);
    assert_eq!(b, b"offline");
    drop(s);
    sleep(Duration::from_millis(50)).await;
    let (mut s, present) = Client::connect(a, "durable", false).await;
    assert!(present);
    let (h, _, _, again) = s.received().await;
    assert_eq!(h, 0x3a);
    assert_eq!(again, id);
    s.send(0x40, &id.to_be_bytes()).await;
    s.disconnect().await;
    let (mut s, present) = Client::connect(a, "durable", true).await;
    assert!(!present);
    assert!(broker.core.durable_subscriptions().is_empty());
    s.disconnect().await;
    p.disconnect().await;
    broker.stop();
}

#[tokio::test]
async fn qos_two_sessions_resume_both_handshake_directions() {
    let broker = runtime::start(config()).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let (mut s, _) = Client::connect(a, "q2s", false).await;
    s.subscribe("qos2", 2).await;
    let (mut p, _) = Client::connect(a, "q2p", false).await;
    p.publish("qos2", b"exact", 2, false, 45).await;
    assert_eq!(p.read().await, (0x50, vec![0, 45]));
    p.disconnect().await;
    let (mut p, _) = Client::connect(a, "q2p", false).await;
    p.send(0x62, &[0, 45]).await;
    assert_eq!(p.read().await, (0x70, vec![0, 45]));
    let (_, _, b, id) = s.received().await;
    assert_eq!(b, b"exact");
    s.send(0x50, &id.to_be_bytes()).await;
    assert_eq!(s.read().await, (0x62, id.to_be_bytes().to_vec()));
    drop(s);
    sleep(Duration::from_millis(50)).await;
    let (mut s, _) = Client::connect(a, "q2s", false).await;
    assert_eq!(s.read().await, (0x62, id.to_be_bytes().to_vec()));
    s.send(0x70, &id.to_be_bytes()).await;
    s.send(0xc0, &[]).await;
    assert_eq!(s.read().await, (0xd0, vec![]));
    s.disconnect().await;
    p.disconnect().await;
    broker.stop();
}

#[tokio::test]
async fn takeover_keepalive_unsubscribe_and_system_wildcards() {
    let broker = runtime::start(config()).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let (mut old, _) = Client::connect(a, "same", true).await;
    let (mut new, _) = Client::connect(a, "same", true).await;
    old.closed().await;
    new.send(0xc0, &[]).await;
    assert_eq!(new.read().await, (0xd0, vec![]));
    new.subscribe("#", 0).await;
    let (internal, _) = broker.core.connect_internal("system");
    broker
        .core
        .send(
            internal,
            Message::text(Destination::topic("$SYS.test"), "hidden"),
            None,
        )
        .unwrap();
    sleep(Duration::from_millis(20)).await;
    new.send(0xc0, &[]).await;
    assert_eq!(new.read().await, (0xd0, vec![]));
    let mut b = vec![0, 2];
    text(&mut b, "#");
    new.send(0xa2, &b).await;
    assert_eq!(new.read().await, (0xb0, vec![0, 2]));
    new.disconnect().await;
    let mut idle = Client::raw(a).await;
    idle.send(0x10, &connect_body(4, "idle", 2, 1)).await;
    assert_eq!(idle.read().await.0, 0x20);
    idle.closed().await;
    broker.core.disconnect(internal);
    broker.stop();
}

#[tokio::test]
async fn framing_limits_and_interleaved_partial_packet() {
    let mut cfg = config();
    cfg.broker.max_frame_size = 100;
    let broker = runtime::start(cfg).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let mut bad = Client::raw(a).await;
    bad.0
        .write_all(&[0x10, 0xff, 0xff, 0xff, 0x7f])
        .await
        .unwrap();
    bad.closed().await;
    let (mut c, _) = Client::connect(a, "fragment", true).await;
    c.subscribe("fragment", 0).await;
    c.0.write_all(&[0xc0]).await.unwrap();
    let (internal, _) = broker.core.connect_internal("partial");
    broker
        .core
        .send(
            internal,
            Message::text(Destination::topic("fragment"), "interleaved"),
            None,
        )
        .unwrap();
    assert_eq!(c.received().await.2, b"interleaved");
    c.0.write_all(&[0]).await.unwrap();
    assert_eq!(c.read().await, (0xd0, vec![]));
    c.disconnect().await;
    broker.core.disconnect(internal);
    broker.stop();
}

#[tokio::test]
async fn durable_session_and_retained_message_survive_restart() {
    let path = std::env::current_dir()
        .unwrap()
        .join("target/tmp")
        .join(format!("mqtt-restart-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    let mut cfg = config();
    cfg.persistence.backend = "sled".into();
    cfg.persistence.path = path.join("sled").to_str().unwrap().into();
    let broker = runtime::start(cfg.clone()).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let (mut s, _) = Client::connect(a, "restart", false).await;
    s.subscribe("restart/topic", 1).await;
    s.disconnect().await;
    let (mut p, _) = Client::connect(a, "restart-pub", true).await;
    p.publish("restart/topic", b"stored", 1, true, 1).await;
    assert_eq!(p.read().await.0, 0x40);
    p.disconnect().await;
    broker.stop();
    sleep(Duration::from_millis(100)).await;
    let broker = runtime::start(cfg).await.unwrap();
    let a = broker.address("mqtt").unwrap();
    let (mut s, present) = Client::connect(a, "restart", false).await;
    assert!(present);
    let (_, _, b, id) = s.received().await;
    assert_eq!(b, b"stored");
    s.send(0x40, &id.to_be_bytes()).await;
    s.disconnect().await;
    let (mut retained, _) = Client::connect(a, "after", true).await;
    retained.subscribe("restart/topic", 1).await;
    assert_eq!(retained.received().await.0, 0x33);
    retained.disconnect().await;
    broker.stop();
    sleep(Duration::from_millis(100)).await;
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn takeover_does_not_restore_a_clean_sessions_subscriptions() {
    let broker = runtime::start(config()).await.unwrap();
    let address = broker.address("mqtt").unwrap();
    let (mut old, _) = Client::connect(address, "clean-takeover", true).await;
    old.subscribe("transient/topic", 1).await;
    let (mut new, present) = Client::connect(address, "clean-takeover", false).await;
    assert!(!present);
    old.closed().await;
    assert!(broker.core.durable_subscriptions().is_empty());
    new.send(0xc0, &[]).await;
    assert_eq!(new.read().await, (0xd0, vec![]));
    new.disconnect().await;
    broker.stop();
}

#[tokio::test]
async fn optional_packet_journal_resumes_qos_two_after_restart() {
    let path = std::env::current_dir()
        .unwrap()
        .join("target/tmp")
        .join(format!("mqtt-qos2-restart-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    let mut cfg = config();
    cfg.persistence.backend = "sled".into();
    cfg.persistence.path = path.join("sled").to_str().unwrap().into();
    cfg.protocols.mqtt.options.insert(
        "session_state_dir".into(),
        toml::Value::String(path.join("sessions").to_str().unwrap().into()),
    );
    let broker = runtime::start(cfg.clone()).await.unwrap();
    let address = broker.address("mqtt").unwrap();
    let (mut sub, _) = Client::connect(address, "restart-q2-sub", false).await;
    sub.subscribe("pending/#", 2).await;
    let (mut publisher, _) = Client::connect(address, "restart-q2-pub", false).await;
    publisher
        .publish("pending/incoming", b"pending", 2, false, 31)
        .await;
    assert_eq!(publisher.read().await, (0x50, vec![0, 31]));
    publisher.disconnect().await;
    let (internal, _) = broker.core.connect_internal("restart-qos2");
    broker
        .core
        .send(
            internal,
            Message::text(Destination::topic("pending.outgoing"), "already-read")
                .with_property("ActiveMQ.MQTT.QoS", 2i32),
            None,
        )
        .unwrap();
    let (_, _, body, id) = sub.received().await;
    assert_eq!(body, b"already-read");
    sub.send(0x50, &id.to_be_bytes()).await;
    assert_eq!(sub.read().await, (0x62, id.to_be_bytes().to_vec()));
    sub.disconnect().await;
    broker.core.disconnect(internal);
    broker.stop();
    sleep(Duration::from_millis(100)).await;
    let broker = runtime::start(cfg).await.unwrap();
    let address = broker.address("mqtt").unwrap();
    let (mut sub, present) = Client::connect(address, "restart-q2-sub", false).await;
    assert!(present);
    assert_eq!(sub.read().await, (0x62, id.to_be_bytes().to_vec()));
    sub.send(0x70, &id.to_be_bytes()).await;
    let (mut publisher, present) = Client::connect(address, "restart-q2-pub", false).await;
    assert!(present);
    publisher.send(0x62, &[0, 31]).await;
    assert_eq!(publisher.read().await, (0x70, vec![0, 31]));
    let (header, _, body, id) = sub.received().await;
    assert_eq!(header, 0x34);
    assert_eq!(body, b"pending");
    sub.send(0x50, &id.to_be_bytes()).await;
    assert_eq!(sub.read().await, (0x62, id.to_be_bytes().to_vec()));
    sub.send(0x70, &id.to_be_bytes()).await;
    sub.disconnect().await;
    publisher.disconnect().await;
    broker.stop();
    sleep(Duration::from_millis(100)).await;
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn subscription_authorization_failure_is_individual_suback_failure() {
    use activemq_classic_rs::core::security::{AuthorizationEntry, UserConfig};
    let mut cfg = config();
    cfg.security.enabled = true;
    cfg.security.users.push(UserConfig {
        username: "reader".into(),
        password: Some("pass".into()),
        groups: vec!["readers".into()],
        ..Default::default()
    });
    cfg.security.authorization = vec![
        AuthorizationEntry {
            destination: "topic://allowed".into(),
            read: vec!["readers".into()],
            admin: vec!["readers".into()],
            ..Default::default()
        },
        AuthorizationEntry {
            destination: "topic://denied".into(),
            read: vec!["admins".into()],
            ..Default::default()
        },
    ];
    let broker = runtime::start(cfg).await.unwrap();
    let mut client = Client::raw(broker.address("mqtt").unwrap()).await;
    let mut body = connect_body(4, "reader", 0xc2, 0);
    text(&mut body, "reader");
    text(&mut body, "pass");
    client.send(0x10, &body).await;
    assert_eq!(client.read().await, (0x20, vec![0, 0]));
    let mut body = vec![0, 3];
    text(&mut body, "allowed");
    body.push(1);
    text(&mut body, "denied");
    body.push(1);
    client.send(0x82, &body).await;
    assert_eq!(client.read().await, (0x90, vec![0, 3, 1, 128]));
    client.disconnect().await;
    broker.stop();
}
