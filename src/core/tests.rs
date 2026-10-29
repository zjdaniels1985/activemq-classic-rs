use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;

use super::scheduler::{SCHEDULED_DELAY, SCHEDULER_ACTION};
use super::security::{AuthorizationEntry, SecurityConfig, SecurityManager, UserConfig};
use super::store::XidRecord;
use super::*;

fn events(rx: &mut UnboundedReceiver<Event>) -> Vec<Event> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

fn deliveries(rx: &mut UnboundedReceiver<Event>) -> Vec<Delivery> {
    events(rx)
        .into_iter()
        .filter_map(|event| match event {
            Event::Deliver(delivery) => Some(delivery),
            _ => None,
        })
        .collect()
}

fn texts(list: &[Delivery]) -> Vec<String> {
    list.iter()
        .map(|d| d.message.text_body().unwrap_or_default().to_owned())
        .collect()
}

fn client(core: &BrokerCore, client_id: Option<&str>) -> (ConnectionId, UnboundedReceiver<Event>) {
    core.connect(ConnectInfo {
        client_id: client_id.map(str::to_owned),
        protocol: "test".to_owned(),
        remote_address: "test".to_owned(),
        redeliver_on_rollback: true,
        ..ConnectInfo::default()
    })
    .expect("connect")
}

fn send_text(core: &BrokerCore, connection: ConnectionId, destination: Destination, text: &str) {
    core.send(connection, Message::text(destination, text), None)
        .expect("send");
}

fn no_advisories() -> Policy {
    Policy {
        advisory_support: false,
        ..Policy::default()
    }
}

fn core_with(policy: Policy) -> Arc<BrokerCore> {
    BrokerCore::open("test", None, policy, SecurityManager::disabled()).expect("broker")
}

fn opts(destination: Destination) -> ConsumerOptions {
    ConsumerOptions::new(destination)
}

#[test]
fn retained_replay_checks_ownership_filters_and_expiration() {
    let core = core_with(no_advisories());
    let (connection, mut rx) = client(&core, None);
    let (other, _) = client(&core, None);
    let destination = Destination::topic("replay.snapshot");
    for (name, color, expiration) in [
        ("match", "blue", 0),
        ("filtered", "red", 0),
        ("expired", "blue", 1),
    ] {
        let mut message = Message::text(destination.clone(), name);
        message
            .properties
            .insert(RETAIN_PROPERTY.into(), Value::Bool(true));
        message.properties.insert("color".into(), color.into());
        message.destination = Destination::topic(format!("replay.{name}"));
        message.expiration = expiration;
        core.send(connection, message, None).unwrap();
    }
    let consumer = core
        .subscribe(
            connection,
            ConsumerOptions {
                selector: Some("color = 'blue'".into()),
                ..opts(Destination::topic("replay.>"))
            },
        )
        .unwrap();
    deliveries(&mut rx);
    assert!(core.replay_retained(other, consumer).is_err());
    core.replay_retained(connection, consumer).unwrap();
    let replayed = deliveries(&mut rx);
    assert_eq!(texts(&replayed), vec!["match"]);
    assert_eq!(replayed[0].consumer, consumer);
    let queue = core
        .subscribe(connection, opts(Destination::queue("not-a-topic")))
        .unwrap();
    assert!(core.replay_retained(connection, queue).is_err());
}

#[test]
fn queue_round_robin_and_ack() {
    let core = core_with(no_advisories());
    let (conn, mut rx) = client(&core, None);
    let queue = Destination::queue("orders");
    let a = core
        .subscribe(
            conn,
            ConsumerOptions {
                prefetch: 1,
                ..opts(queue.clone())
            },
        )
        .unwrap();
    core.subscribe(
        conn,
        ConsumerOptions {
            prefetch: 1,
            ..opts(queue.clone())
        },
    )
    .unwrap();
    for i in 0..3 {
        send_text(&core, conn, queue.clone(), &format!("m{i}"));
    }
    let first = deliveries(&mut rx);
    assert_eq!(first.len(), 2);
    assert_ne!(first[0].consumer, first[1].consumer);
    let to_ack = first.iter().find(|d| d.consumer == a).unwrap();
    core.ack(
        conn,
        a,
        Acks::Deliveries(vec![to_ack.delivery_id]),
        AckOp::Consumed,
        None,
    )
    .unwrap();
    let second = deliveries(&mut rx);
    assert_eq!(texts(&second), vec!["m2"]);
    assert_eq!(second[0].consumer, a);
    let stats = core.destinations();
    let orders = stats.iter().find(|s| s.uri == "queue://orders").unwrap();
    assert_eq!(orders.dequeued, 1);
    assert_eq!(orders.in_flight, 2);
}

#[test]
fn priority_ordering_and_redelivery_to_dlq() {
    let core = core_with(Policy {
        max_redeliveries: 1,
        ..no_advisories()
    });
    let (conn, mut rx) = client(&core, None);
    let queue = Destination::queue("q");
    let mut low = Message::text(queue.clone(), "low");
    low.priority = 1;
    let mut high = Message::text(queue.clone(), "high");
    high.priority = 9;
    core.send(conn, low, None).unwrap();
    core.send(conn, high, None).unwrap();
    let consumer = core
        .subscribe(
            conn,
            ConsumerOptions {
                prefetch: 1,
                ..opts(queue.clone())
            },
        )
        .unwrap();
    assert_eq!(texts(&deliveries(&mut rx)), vec!["high"]);
    core.ack(
        conn,
        consumer,
        Acks::All,
        AckOp::Release { increment: true },
        None,
    )
    .unwrap();
    let again = deliveries(&mut rx);
    assert_eq!(texts(&again), vec!["high"]);
    assert!(again[0].redelivered);
    core.ack(
        conn,
        consumer,
        Acks::All,
        AckOp::Release { increment: true },
        None,
    )
    .unwrap();
    assert_eq!(texts(&deliveries(&mut rx)), vec!["low"]);
    let dlq = Destination::queue("ActiveMQ.DLQ");
    let dead = core.browse(&dlq, 10).unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].body, "high");
    assert!(core.retry_message(&dlq, &dead[0].message_id).unwrap());
    assert_eq!(core.browse(&queue, 10).unwrap().len(), 1);
}

#[test]
fn topics_wildcards_selectors_and_no_local() {
    let core = core_with(no_advisories());
    let (publisher, mut prx) = client(&core, None);
    let (subscriber, mut srx) = client(&core, None);
    let nyse = Destination::topic("prices.stock.nyse");
    core.subscribe(subscriber, opts(Destination::topic("prices.>")))
        .unwrap();
    core.subscribe(
        subscriber,
        ConsumerOptions {
            selector: Some("symbol = 'IBM' AND price > 100".to_owned()),
            ..opts(Destination::topic("prices.stock.*"))
        },
    )
    .unwrap();
    core.subscribe(
        publisher,
        ConsumerOptions {
            no_local: true,
            ..opts(nyse.clone())
        },
    )
    .unwrap();
    for (text, price) in [("a", 120), ("b", 90)] {
        let message = Message::text(nyse.clone(), text)
            .with_property("symbol", "IBM")
            .with_property("price", price);
        core.send(publisher, message, None).unwrap();
    }
    assert_eq!(deliveries(&mut srx).len(), 3);
    assert!(deliveries(&mut prx).is_empty());
}

#[test]
fn durable_subscription_survives_restart() {
    let dir = std::env::temp_dir().join(format!("amq-core-durable-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let topic = Destination::topic("news");
    let durable = || ConsumerOptions {
        durable_name: Some("sub".into()),
        ..opts(Destination::topic("news"))
    };
    {
        let core = BrokerCore::open(
            "test",
            Some(&dir),
            no_advisories(),
            SecurityManager::disabled(),
        )
        .unwrap();
        let (conn, _rx) = client(&core, Some("c1"));
        let consumer = core.subscribe(conn, durable()).unwrap();
        assert!(core.subscribe(conn, durable()).is_err());
        core.close_consumer(consumer);
        let (publisher, _p) = client(&core, None);
        send_text(&core, publisher, topic.clone(), "offline");
        core.shutdown();
    }
    let core = BrokerCore::open(
        "test",
        Some(&dir),
        no_advisories(),
        SecurityManager::disabled(),
    )
    .unwrap();
    assert_eq!(core.durable_subscriptions()[0].pending, 1);
    let (conn, mut rx) = client(&core, Some("c1"));
    core.subscribe(conn, durable()).unwrap();
    assert_eq!(texts(&deliveries(&mut rx)), vec!["offline"]);
    core.disconnect(conn);
    let (conn, _rx) = client(&core, Some("c1"));
    core.remove_durable(conn, "sub").unwrap();
    assert!(core.durable_subscriptions().is_empty());
    core.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn local_transactions_commit_and_rollback() {
    let core = core_with(no_advisories());
    let (conn, mut rx) = client(&core, None);
    let queue = Destination::queue("tx");
    let tx = TxId::Local("t1".into());
    core.begin(conn, tx.clone()).unwrap();
    core.send(conn, Message::text(queue.clone(), "one"), Some(&tx))
        .unwrap();
    assert!(core.browse(&queue, 10).unwrap_or_default().is_empty());
    core.commit(conn, &tx, true).unwrap();
    let consumer = core.subscribe(conn, opts(queue.clone())).unwrap();
    assert_eq!(texts(&deliveries(&mut rx)), vec!["one"]);
    let tx2 = TxId::Local("t2".into());
    core.begin(conn, tx2.clone()).unwrap();
    core.ack(conn, consumer, Acks::All, AckOp::Consumed, Some(&tx2))
        .unwrap();
    core.rollback(conn, &tx2).unwrap();
    let again = deliveries(&mut rx);
    assert_eq!(texts(&again), vec!["one"]);
    assert!(again[0].redelivered);
    let tx3 = TxId::Local("t3".into());
    core.begin(conn, tx3.clone()).unwrap();
    core.ack(conn, consumer, Acks::All, AckOp::Consumed, Some(&tx3))
        .unwrap();
    core.commit(conn, &tx3, true).unwrap();
    let stats = core.destinations();
    let q = stats.iter().find(|s| s.uri == "queue://tx").unwrap();
    assert_eq!((q.pending, q.in_flight, q.dequeued), (0, 0, 1));
}

#[test]
fn xa_prepare_survives_restart() {
    let dir = std::env::temp_dir().join(format!("amq-core-xa-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let queue = Destination::queue("xa");
    let xid = TxId::Xa(XidRecord {
        format_id: 1,
        global_id: vec![1, 2],
        branch_id: vec![3],
    });
    {
        let core = BrokerCore::open(
            "test",
            Some(&dir),
            no_advisories(),
            SecurityManager::disabled(),
        )
        .unwrap();
        let (conn, _rx) = client(&core, None);
        core.begin(conn, xid.clone()).unwrap();
        core.send(conn, Message::text(queue.clone(), "xa-msg"), Some(&xid))
            .unwrap();
        assert!(!core.prepare(conn, &xid).unwrap());
        core.shutdown();
    }
    let core = BrokerCore::open(
        "test",
        Some(&dir),
        no_advisories(),
        SecurityManager::disabled(),
    )
    .unwrap();
    assert_eq!(core.recover().len(), 1);
    let (conn, mut rx) = client(&core, None);
    core.commit(conn, &xid, false).unwrap();
    core.subscribe(conn, opts(queue.clone())).unwrap();
    assert_eq!(texts(&deliveries(&mut rx)), vec!["xa-msg"]);
    assert!(core.recover().is_empty());
    core.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scheduled_delivery_and_management() {
    let core = core_with(no_advisories());
    let (conn, mut rx) = client(&core, None);
    let queue = Destination::queue("later");
    core.subscribe(conn, opts(queue.clone())).unwrap();
    let delayed = Message::text(queue.clone(), "delayed").with_property(SCHEDULED_DELAY, 150i64);
    core.send(conn, delayed, None).unwrap();
    let removed =
        Message::text(queue.clone(), "removed").with_property(SCHEDULED_DELAY, 100_000i64);
    core.send(conn, removed, None).unwrap();
    assert!(deliveries(&mut rx).is_empty());
    assert_eq!(core.jobs().len(), 2);
    let far = core
        .jobs()
        .into_iter()
        .find(|j| j.delay == 100_000)
        .unwrap();
    let request = Message::new(
        Destination::topic(destination::SCHEDULER_MANAGEMENT_TOPIC),
        Body::Empty,
    )
    .with_property(SCHEDULER_ACTION, "REMOVE")
    .with_property(scheduler::SCHEDULED_ID, far.id.as_str());
    core.send(conn, request, None).unwrap();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(texts(&deliveries(&mut rx)), vec!["delayed"]);
    assert!(core.jobs().is_empty());
}

#[test]
fn security_authentication_and_authorization() {
    let user = |name: &str, password: &str, group: &str| UserConfig {
        username: name.into(),
        password: Some(password.into()),
        groups: vec![group.into()],
        ..Default::default()
    };
    let security = SecurityManager::new(SecurityConfig {
        enabled: true,
        allow_anonymous: false,
        users: vec![
            user("admin", "secret", "admins"),
            user("reader", "pw", "readers"),
        ],
        authorization: vec![AuthorizationEntry {
            destination: "queue://>".into(),
            read: vec!["admins".into(), "readers".into()],
            write: vec!["admins".into()],
            admin: vec!["admins".into()],
        }],
        ..Default::default()
    })
    .unwrap();
    let core = BrokerCore::open("test", None, no_advisories(), security).unwrap();
    let login = |name: &str, password: &str| ConnectInfo {
        username: Some(name.into()),
        password: Some(password.into()),
        ..Default::default()
    };
    assert!(matches!(
        core.connect(login("admin", "nope")),
        Err(CoreError::Security(_))
    ));
    assert!(core.connect(ConnectInfo::default()).is_err());
    let (admin, _a) = core.connect(login("admin", "secret")).unwrap();
    let (reader, mut rrx) = core.connect(login("reader", "pw")).unwrap();
    assert!(matches!(
        core.subscribe(reader, opts(Destination::queue("new"))),
        Err(CoreError::Security(_))
    ));
    send_text(&core, admin, Destination::queue("secure"), "hi");
    assert!(matches!(
        core.send(
            reader,
            Message::text(Destination::queue("secure"), "x"),
            None
        ),
        Err(CoreError::Security(_))
    ));
    core.subscribe(reader, opts(Destination::queue("secure")))
        .unwrap();
    assert_eq!(texts(&deliveries(&mut rrx)), vec!["hi"]);
}

#[test]
fn temp_destinations_virtual_topics_and_browsers() {
    let core = core_with(no_advisories());
    let (conn, mut rx) = client(&core, None);
    let temp = core.new_temp(conn, DestinationKind::TempQueue).unwrap();
    core.subscribe(conn, opts(temp.clone())).unwrap();
    send_text(&core, conn, temp.clone(), "reply");
    assert_eq!(texts(&deliveries(&mut rx)), vec!["reply"]);
    let (other, _o) = client(&core, None);
    core.disconnect(conn);
    assert!(core.send(other, Message::text(temp, "late"), None).is_err());

    let (conn, mut rx) = client(&core, None);
    let a = Destination::queue("Consumer.A.VirtualTopic.Orders");
    let b = Destination::queue("Consumer.B.VirtualTopic.Orders");
    core.create_destination(&a).unwrap();
    core.create_destination(&b).unwrap();
    send_text(
        &core,
        conn,
        Destination::topic("VirtualTopic.Orders"),
        "order",
    );
    assert_eq!(core.browse(&a, 10).unwrap().len(), 1);
    assert_eq!(core.browse(&b, 10).unwrap().len(), 1);
    let browser = core
        .subscribe(
            conn,
            ConsumerOptions {
                browser: true,
                ..opts(a.clone())
            },
        )
        .unwrap();
    let browse_events = events(&mut rx);
    assert_eq!(browse_events.len(), 2);
    assert!(
        matches!(browse_events.last(), Some(Event::BrowseDone { consumer }) if *consumer == browser)
    );
    assert_eq!(core.browse(&a, 10).unwrap().len(), 1);
}

#[test]
fn message_groups_and_exclusive_consumers() {
    let core = core_with(no_advisories());
    let (conn, mut rx) = client(&core, None);
    let queue = Destination::queue("groups");
    core.subscribe(conn, opts(queue.clone())).unwrap();
    core.subscribe(conn, opts(queue.clone())).unwrap();
    for _ in 0..4 {
        let mut message = Message::text(queue.clone(), "g");
        message.group_id = Some("G1".into());
        core.send(conn, message, None).unwrap();
    }
    let got = deliveries(&mut rx);
    assert_eq!(got.len(), 4);
    assert!(got.iter().all(|d| d.consumer == got[0].consumer));
    let excl = Destination::queue("excl");
    let e1 = core
        .subscribe(
            conn,
            ConsumerOptions {
                exclusive: true,
                ..opts(excl.clone())
            },
        )
        .unwrap();
    core.subscribe(conn, opts(excl.clone())).unwrap();
    for _ in 0..3 {
        send_text(&core, conn, excl.clone(), "x");
    }
    let got = deliveries(&mut rx);
    assert_eq!(got.len(), 3);
    assert!(got.iter().all(|d| d.consumer == e1));
}

#[test]
fn pull_consumer_and_credit() {
    let core = core_with(no_advisories());
    let (conn, mut rx) = client(&core, None);
    let queue = Destination::queue("pull");
    let consumer = core
        .subscribe(
            conn,
            ConsumerOptions {
                prefetch: 0,
                ..opts(queue.clone())
            },
        )
        .unwrap();
    send_text(&core, conn, queue.clone(), "p");
    assert!(deliveries(&mut rx).is_empty());
    core.pull(consumer, -1);
    assert_eq!(texts(&deliveries(&mut rx)), vec!["p"]);
    core.ack(conn, consumer, Acks::All, AckOp::Consumed, None)
        .unwrap();
    core.pull(consumer, -1);
    assert!(matches!(
        events(&mut rx).as_slice(),
        [Event::PullEmpty { .. }]
    ));

    let credit_queue = Destination::queue("credit");
    let link = core
        .subscribe(
            conn,
            ConsumerOptions {
                credit_mode: true,
                ..opts(credit_queue.clone())
            },
        )
        .unwrap();
    send_text(&core, conn, credit_queue.clone(), "c1");
    send_text(&core, conn, credit_queue.clone(), "c2");
    assert!(deliveries(&mut rx).is_empty());
    core.set_credit(link, 1);
    assert_eq!(texts(&deliveries(&mut rx)), vec!["c1"]);
    core.set_credit(link, 5);
    core.drain(link);
    let drained = events(&mut rx);
    assert!(matches!(
        drained.last(),
        Some(Event::Drained { credit: 4, .. })
    ));
}

#[test]
fn expired_messages_go_to_dlq() {
    let core = core_with(Policy {
        expire_check_interval_ms: 10,
        ..no_advisories()
    });
    let (conn, _rx) = client(&core, None);
    let mut message = Message::text(Destination::queue("exp"), "old");
    message.expiration = message::now_millis() + 50;
    core.send(conn, message, None).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        core.browse(&Destination::queue("exp"), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.browse(&Destination::queue("ActiveMQ.DLQ"), 10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn advisories_and_client_id_uniqueness() {
    let core = BrokerCore::in_memory();
    let (watcher, mut wrx) = client(&core, None);
    let (conn, _rx) = client(&core, Some("unique"));
    let duplicate = ConnectInfo {
        client_id: Some("unique".into()),
        ..Default::default()
    };
    assert!(matches!(
        core.connect(duplicate),
        Err(CoreError::InvalidClientId(_))
    ));
    core.subscribe(conn, opts(Destination::queue("watched")))
        .unwrap();
    core.subscribe(
        watcher,
        opts(Destination::topic("ActiveMQ.Advisory.Consumer.>")),
    )
    .unwrap();
    let replayed = deliveries(&mut wrx);
    assert!(
        replayed
            .iter()
            .any(|d| matches!(d.message.advisory, Some(AdvisoryData::ConsumerAdded { .. })))
    );
    core.disconnect(conn);
    let removed = deliveries(&mut wrx);
    assert!(removed.iter().any(|d| matches!(
        d.message.advisory,
        Some(AdvisoryData::ConsumerRemoved { .. })
    )));
    let takeover = ConnectInfo {
        client_id: Some("unique".into()),
        takeover: true,
        ..Default::default()
    };
    core.connect(takeover).unwrap();
}

#[test]
fn retained_messages() {
    let core = core_with(no_advisories());
    let (conn, mut rx) = client(&core, None);
    let topic = Destination::topic("sensors.temp");
    let message = Message::text(topic.clone(), "21C").with_property(RETAIN_PROPERTY, true);
    core.send(conn, message, None).unwrap();
    core.subscribe(conn, opts(Destination::topic("sensors.*")))
        .unwrap();
    let got = deliveries(&mut rx);
    assert_eq!(texts(&got), vec!["21C"]);
    assert_eq!(
        got[0]
            .message
            .property(RETAINED_PROPERTY)
            .and_then(Value::as_bool),
        Some(true)
    );
}
