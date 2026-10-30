//! Bounded core send benchmark; not a wire-protocol or Classic comparison.

use std::error::Error;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use activemq_classic_rs::core::security::SecurityManager;
use activemq_classic_rs::core::store::StoreOptions;
use activemq_classic_rs::core::{BrokerCore, ConnectInfo, Destination, Message, Policy};

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() > 5 {
        return Err("usage: throughput [memory|sled] [producers] [messages/producer] [payload-bytes] [group-delay-us]".into());
    }
    let backend = args.first().map(String::as_str).unwrap_or("memory");
    let number = |index: usize, default: usize| -> Result<usize, Box<dyn Error>> {
        Ok(args
            .get(index)
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(default))
    };
    let producers = number(1, 8)?;
    let messages = number(2, 500)?;
    let payload_bytes = number(3, 256)?;
    let delay = number(4, 0)?;
    if !matches!(backend, "memory" | "sled")
        || !(1..=256).contains(&producers)
        || messages == 0
        || payload_bytes == 0
        || producers
            .saturating_mul(messages)
            .saturating_mul(payload_bytes.max(1024))
            > 256 * 1024 * 1024
    {
        return Err("invalid workload: use memory|sled, 1..256 producers, positive counts/sizes, and at most 256 MiB estimated queued data".into());
    }
    let options = StoreOptions {
        max_delay_us: delay.try_into()?,
        ..StoreOptions::default()
    };
    options.validate()?;
    let path = (backend == "sled").then(|| {
        std::env::temp_dir().join(format!(
            "activemq-throughput-{}-{}",
            std::process::id(),
            activemq_classic_rs::core::message::now_millis()
        ))
    });
    if let Some(path) = &path {
        std::fs::create_dir(path)?;
    }
    let core = BrokerCore::open_with_store_options(
        "throughput",
        path.as_deref(),
        Policy {
            advisory_support: false,
            ..Policy::default()
        },
        SecurityManager::disabled(),
        options,
    )?;
    let barrier = Arc::new(Barrier::new(producers + 1));
    let mut workers = Vec::new();
    let mut clients = Vec::new();
    for producer in 0..producers {
        let (connection, events) = core.connect(ConnectInfo::default())?;
        let destination = Destination::queue(format!("benchmark.{producer}"));
        core.create_destination(&destination)?;
        clients.push((
            connection,
            events,
            Message::text(destination, "x".repeat(payload_bytes)),
        ));
    }
    for (connection, _events, template) in clients {
        let core = Arc::clone(&core);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let _events = _events;
            let mut latencies = Vec::with_capacity(messages);
            barrier.wait();
            for _ in 0..messages {
                let start = Instant::now();
                let mut message = template.clone();
                message.message_id = activemq_classic_rs::core::message::generate_message_id();
                core.send(connection, message, None)?;
                latencies.push(start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
            }
            core.disconnect(connection);
            Ok::<_, activemq_classic_rs::core::CoreError>(latencies)
        }));
    }
    let start = Instant::now();
    barrier.wait();
    let mut latencies = Vec::with_capacity(producers * messages);
    for worker in workers {
        latencies.extend(worker.join().map_err(|_| "benchmark worker panicked")??);
    }
    let elapsed = start.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let percentile = |percent: usize| {
        latencies[(latencies.len() * percent).div_ceil(100).saturating_sub(1)] as f64 / 1000.0
    };
    core.shutdown();
    let info = core.info();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "workload": "persistent core sends, independent queues, no consumers",
            "backend": backend, "producers": producers, "messages": latencies.len(),
            "payload_bytes": payload_bytes, "group_delay_us": delay,
            "elapsed_seconds": elapsed, "messages_per_second": latencies.len() as f64 / elapsed,
            "send_latency_us": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99)},
            "state_lock": info.state_lock, "storage": info.storage,
        }))?
    );
    drop(core);
    if let Some(path) = path {
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}
