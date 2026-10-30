use std::path::Path;

use activemq_classic_rs::core::security::SecurityManager;
use activemq_classic_rs::core::store::XidRecord;
use activemq_classic_rs::core::{
    AckOp, Acks, BrokerCore, ConnectInfo, ConsumerOptions, Destination, Message, Policy, TxId,
};

fn open(path: &Path) -> std::sync::Arc<BrokerCore> {
    BrokerCore::open(
        "crash-test",
        Some(path),
        Policy {
            advisory_support: false,
            ..Policy::default()
        },
        SecurityManager::disabled(),
    )
    .unwrap()
}

fn transaction(id: u8) -> TxId {
    TxId::Xa(XidRecord {
        format_id: 42,
        global_id: vec![id],
        branch_id: vec![1],
    })
}

#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("AMQ_CRASH_TEST_DIR") else {
        return;
    };
    let core = open(Path::new(&path));
    let (connection, mut events) = core
        .connect(ConnectInfo {
            client_id: Some("durable-client".into()),
            ..ConnectInfo::default()
        })
        .unwrap();
    let source = Destination::queue("source");
    core.send(
        connection,
        Message::text(source.clone(), "consume atomically"),
        None,
    )
    .unwrap();
    let consumer = core
        .subscribe(connection, ConsumerOptions::new(source))
        .unwrap();
    assert!(matches!(
        events.try_recv(),
        Ok(activemq_classic_rs::core::Event::Deliver(_))
    ));
    let committed = transaction(1);
    core.begin(connection, committed.clone()).unwrap();
    core.ack(
        connection,
        consumer,
        Acks::All,
        AckOp::Consumed,
        Some(&committed),
    )
    .unwrap();
    for name in ["committed.a", "committed.b"] {
        core.send(
            connection,
            Message::text(Destination::queue(name), "committed"),
            Some(&committed),
        )
        .unwrap();
    }
    assert!(!core.prepare(connection, &committed).unwrap());
    core.commit(connection, &committed, false).unwrap();

    let prepared = transaction(2);
    core.begin(connection, prepared.clone()).unwrap();
    core.send(
        connection,
        Message::text(Destination::queue("prepared"), "in doubt"),
        Some(&prepared),
    )
    .unwrap();
    assert!(!core.prepare(connection, &prepared).unwrap());
    let mut durable = ConsumerOptions::new(Destination::topic("durable-topic"));
    durable.durable_name = Some("durable-subscription".into());
    core.subscribe(connection, durable).unwrap();
    core.send(
        connection,
        Message::text(Destination::topic("durable-topic"), "retained")
            .with_property("ActiveMQ.Retain", true),
        None,
    )
    .unwrap();
    core.send(
        connection,
        Message::text(Destination::queue("scheduled"), "later")
            .with_property("AMQ_SCHEDULED_DELAY", 3_600_000i64),
        None,
    )
    .unwrap();
    // Exit without running destructors or BrokerCore::shutdown. Every preceding
    // successful durable operation must already be recoverable.
    std::process::exit(0);
}

#[test]
fn successful_durable_operations_survive_exit_without_shutdown() {
    let path = std::env::temp_dir().join(format!(
        "activemq-crash-{}-{}",
        std::process::id(),
        activemq_classic_rs::core::message::now_millis()
    ));
    std::fs::create_dir(&path).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("AMQ_CRASH_TEST_DIR", &path)
        .status()
        .unwrap();
    assert!(status.success(), "crash-test setup failed: {status}");
    let core = open(&path);
    for name in ["committed.a", "committed.b"] {
        assert_eq!(core.browse(&Destination::queue(name), 10).unwrap().len(), 1);
    }
    assert!(
        core.browse(&Destination::queue("source"), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.recover(),
        vec![match transaction(2) {
            TxId::Xa(xid) => xid,
            _ => unreachable!(),
        }]
    );
    assert_eq!(core.durable_subscriptions().len(), 1);
    assert_eq!(core.jobs().len(), 1);
    let (connection, mut events) = core.connect(ConnectInfo::default()).unwrap();
    core.subscribe(
        connection,
        ConsumerOptions::new(Destination::topic("durable-topic")),
    )
    .unwrap();
    assert!(matches!(
        events.try_recv(),
        Ok(activemq_classic_rs::core::Event::Deliver(_))
    ));
    core.commit(connection, &transaction(2), false).unwrap();
    assert_eq!(
        core.browse(&Destination::queue("prepared"), 10)
            .unwrap()
            .len(),
        1
    );
    core.shutdown();
    drop(core);
    std::fs::remove_dir_all(path).unwrap();
}
