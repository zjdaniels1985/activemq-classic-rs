# activemq-classic-rs

A runnable Rust message broker targeting Apache ActiveMQ Classic wire-protocol
compatibility. Existing Java clients use compatible protocols; the Rust-native
API is `core::BrokerCore`, not a translation of the Java JMS API.

**This is not yet a drop-in replacement for ActiveMQ Classic.** Compatibility
is evolving; do not expose this development broker to untrusted networks.

## Implemented

- Queues and topics, wildcard/composite destinations, durable subscriptions,
  temporary destinations, virtual topics, noLocal and queue browsers.
- SQL-92 selectors, message priority, exclusive consumers, message groups,
  expiration, redelivery and dead-letter queues.
- Local transactions and recoverable prepared XA transactions.
- Username/password authentication and destination/group authorization.
- Persistent scheduled delivery (`AMQ_SCHEDULED_DELAY`, `PERIOD`, `REPEAT`,
  `CRON`) and scheduler management.
- Sled persistence and restart recovery; an optional in-memory backend.
- OpenWire with loose encoding, STOMP 1.0–1.2, MQTT 3.1/3.1.1 and AMQP 1.0
  TCP adapters.
- Static and demand-driven OpenWire broker bridges, including destination
  exclusions, duplex forwarding, conduit/non-conduit subscriptions, temporary
  queue replies, remote authentication, URI failover and broker-path/TTL filtering.
- An embedded Rust/Axum administrative web console and JSON API.
- Configurable structured logging with text/JSON output and optional log files.

OpenWire interoperability has been exercised with the ActiveMQ 6.2.6 Java
client, including topics, transactions, durable subscriptions, selectors,
temporary destinations, scheduling and XA. STOMP and MQTT interoperability
harnesses use `stomp.py` and `paho-mqtt`. AMQP checks use Qpid Proton, including
transactional send commit/rollback and durable subscription reconnect with
offline delivery. These checks do not establish complete Classic behavioral
parity.

## Feature comparison

This compares the current Rust implementation with the documented Apache
ActiveMQ Classic and Apache ActiveMQ Artemis product families, not a claim
of equivalent maturity or certification. Apache features depend on release,
transport, plugins and configuration; see the sources below.
**Partial** means a subset is implemented or interoperability coverage is
limited. **Unsupported** means this repository does not implement the feature.

| Feature | activemq-classic-rs (this repository) | Apache ActiveMQ Classic | Apache ActiveMQ Artemis |
|---|---|---|---|
| Broker runtime / native API | Rust / `BrokerCore`; no Rust translation of JMS | Java / JMS clients | Java / Core and JMS clients |
| Existing Java JMS clients | Partial: ActiveMQ 6.2.6 client interoperability exercised; not certified JMS parity | Native ActiveMQ clients | OpenWire compatibility and Artemis JMS clients |
| OpenWire | Partial: v4–v12 loose encoding; v1–v3 rejected; not every command supported | Native protocol | Compatibility protocol for Classic 5.12+ clients |
| STOMP | 1.0–1.2 TCP; typed map-JSON input; no XML/object transformation | STOMP with Classic extensions and transformations | 1.0–1.2 |
| AMQP | Partial: AMQP 1.0 TCP; Proton transactions and durable reconnect exercised | AMQP 1.0 | AMQP 1.0 |
| MQTT | 3.1/3.1.1; retained replay on repeated SUBSCRIBE; no MQTT 5 | 3.1/3.1.1 | 3.1/3.1.1 and 5 |
| Queues, topics, durable and temporary destinations | Implemented; temporary topic bridging unverified | Supported | Supported through address/queue and protocol mappings |
| Selectors and destination routing | SQL-92 selectors, wildcard/composite destinations and virtual topics; no XPath | SQL-92, XPath, wildcard/composite and virtual destinations | SQL-style filters, wildcard addresses and configurable OpenWire virtual-topic mapping |
| Transactions | Local and recoverable prepared XA; protocol-specific interoperability coverage | Local and XA | Local and XA |
| Delayed/scheduled delivery | Classic delay/period/repeat/cron properties and scheduler management | Optional persistent scheduler, including cron/repeat | Scheduled delivery using `_AMQ_SCHED_DELIVERY`; different scheduling model |
| Expiry, DLQ, groups and exclusive consumers | Implemented in core | Supported | Supported |
| Persistence | Sled or memory; no KahaDB file compatibility | KahaDB default; JDBC option | File journal default; JDBC option |
| Authentication / authorization | Basic users, passwords/digests and destination/group rules; no JAAS/plugin parity | Simple authentication, JAAS and authorization plugins | JAAS/custom security managers and role permissions |
| Administration / logging | Rust web console, JSON API, structured text/JSON/file logging; no JMX | Web console, JMX/Jolokia and Java logging | Web console, JMX/Jolokia, management API and Java logging |
| Broker networking | Partial: OpenWire static/demand, duplex, conduit/non-conduit, TTL/exclusions, temp queue proxies, selected advisories and reconnect | Networks of brokers, duplex, conduit options and discovery | Core bridges, clustering and federation; not identical to Classic networks |
| Bridge delivery guarantee | At-least-once; response loss can duplicate forwarding | Depends on bridge, failover and persistence configuration; not a universal distributed transaction | Bridges support reconnect and configurable duplicate detection |
| TLS / WebSocket | Unsupported natively; trusted network or external TLS termination required | Supported transports | Supported transports |
| Discovery, HA and replication | Unsupported; reconnecting a connector is not broker HA | Discovery and shared-store/shared-filesystem or JDBC master/slave configurations | Discovery, shared-store HA and replication |
| Performance comparison | Core microbenchmark available; no measured superiority over Apache brokers | Established production broker | Established production broker |

Apache sources: [Classic protocols][classic-protocols], [MQTT][classic-mqtt],
[networks of brokers][classic-networks], [KahaDB][classic-kahadb],
[master/slave][classic-ha]; Artemis [protocol interoperability][artemis-protocols],
[OpenWire][artemis-openwire], [persistence][artemis-persistence],
[HA][artemis-ha], [bridges][artemis-bridges],
[scheduling][artemis-scheduling], [security][artemis-security],
[management][artemis-management] and [transports][artemis-transports].

[classic-protocols]: https://activemq.apache.org/components/classic/documentation/protocols
[classic-mqtt]: https://activemq.apache.org/components/classic/documentation/mqtt
[classic-networks]: https://activemq.apache.org/components/classic/documentation/networks-of-brokers
[classic-kahadb]: https://activemq.apache.org/components/classic/documentation/kahadb
[classic-ha]: https://activemq.apache.org/components/classic/documentation/masterslave
[artemis-protocols]: https://activemq.apache.org/components/artemis/documentation/latest/protocols-interoperability.html
[artemis-openwire]: https://activemq.apache.org/components/artemis/documentation/latest/openwire.html
[artemis-persistence]: https://activemq.apache.org/components/artemis/documentation/latest/persistence.html
[artemis-ha]: https://activemq.apache.org/components/artemis/documentation/latest/ha.html
[artemis-bridges]: https://activemq.apache.org/components/artemis/documentation/latest/core-bridges.html
[artemis-scheduling]: https://activemq.apache.org/components/artemis/documentation/latest/scheduled-messages.html
[artemis-security]: https://activemq.apache.org/components/artemis/documentation/latest/security.html
[artemis-management]: https://activemq.apache.org/components/artemis/documentation/latest/management.html
[artemis-transports]: https://activemq.apache.org/components/artemis/documentation/latest/configuring-transports.html

## Build and run

Use a current stable Rust toolchain supporting edition 2024 and let-chains.

```sh
cargo build --release
cargo run -- --config activemq-rust.toml
cargo run -- --bind 127.0.0.1:61617 --data-dir ./data/test
cargo run -- --in-memory
```

The executable loads `./activemq-rust.toml` when present, otherwise built-in
defaults. `--help` describes command-line overrides. Default listener addresses:

| Transport | Address |
|---|---|
| OpenWire | `127.0.0.1:61616` |
| STOMP | `127.0.0.1:61613` |
| MQTT | `127.0.0.1:1883` |
| AMQP 1.0 | `127.0.0.1:5672` |
| Web console | `http://127.0.0.1:8161/` |

Set `enabled = false` for any unused protocol or console. Sled defaults to
`./data/sled`; give each broker its own data directory.

## Configuration and security

`activemq-rust.toml` documents broker identity, frame/connection limits,
destination policies, authentication/authorization, protocol-specific options,
persistence, the console, logging and network connectors. Unknown protocol
sections fail validation rather than being silently accepted.

Security is disabled by default for local development. Enable `[security]`,
configure users and explicit `queue://`, `topic://`, `temp-queue://` or
`temp-topic://` authorization patterns before accepting remote clients.
Creating destinations requires admin authorization as well as read/write
access. Passwords may be configured directly or as SHA-256 digests; these
unsalted digests are not a substitute for a production credential store.

MQTT persistent-session subscriptions and queued persistent messages use Sled.
Set `protocols.mqtt.options.session_state_dir` to a broker-specific directory
to also journal packet identifiers and QoS handshake state across restarts.
Back up that directory together with the Sled store. See
[`src/protocols/mqtt/README.md`](src/protocols/mqtt/README.md) for wire-session
persistence guarantees and limitations.

Logging uses `[logging]` (`level`, `format = "text" | "json"`, `file`, `ansi`).
`RUST_LOG` overrides the configured level, for example:

```sh
RUST_LOG=info,activemq_classic_rs=debug cargo run
```

## Administration

The console supports destination creation/deletion, browsing and sending,
purge, message removal/move/DLQ retry, connections, consumers, durable
subscriptions and scheduled jobs. Its REST API is under `/api`:
`/broker`, `/destinations`, `/connections`, `/consumers`, `/subscriptions`
and `/jobs`. Destination-specific routes use
`/api/destinations/{kind}/{name}`; names and message IDs must be URL-encoded.

A non-loopback console bind requires a nonempty `web_console.admin_token`.
The browser's **Admin token** button sets the bearer token for API calls;
non-browser callers use `Authorization: Bearer TOKEN`. The token grants full
administrative access, independently of messaging user permissions.
Transports and the console use plaintext TCP/HTTP; use trusted networks or
appropriate TLS termination.

## Performance and durability

Persistent operations now enqueue ordered storage records, release the
broker-state mutex, and wait for a dedicated Sled group-commit worker.
Concurrent operations can share one flush. Message deliveries and successful
durable confirmations are released only after that flush succeeds.
Message, destination, durable-subscription, scheduled-job, retained and prepared
XA changes belonging to an operation are committed atomically across their Sled
trees. Single-tree operations use Sled's atomic batch fast path. Shutdown drains
accepted storage work. A storage failure suppresses pending deliveries, returns
errors to waiting callers and fences further writes; restart/recovery is required.

`[persistence.group_commit]` controls the pending request count, pending serialized
record bytes, group operation/byte targets and maximum batching delay. The
defaults are a 64-request queue, a 64 MiB pending-record budget, groups targeting
64 operations / 4 MiB, and **zero deliberate delay**. Requests already waiting are
still batched. Increasing `max_delay_us` may reduce flush count, but can worsen
both latency and throughput on fast storage. Every successful durable operation
still waits for a flush, including when the delay is zero.

The worker applies backpressure instead of accepting an unlimited storage backlog.
One atomic operation is never split: an operation larger than the pending-byte
budget may occupy it alone, and group byte limits are targets, not hard message
size caps. Backpressure can temporarily extend state-lock hold times when the
storage budget/queue is exhausted. These limits are **not a broker-wide memory
cap**; destination policy, prefetch and connection limits must also be configured.

Each network connector reuses a forwarding connection and producer, with a
capacity-one request queue, a dedicated cancellation-safe socket reader,
heartbeats and correlated remote confirmations. It acknowledges the source only
after remote acceptance. Reverse duplex demand still uses separate connections
per destination (or per consumer with non-conduit subscriptions); temporary
proxies retain their dedicated ownership lifetimes.

The mutation mutex still exists to preserve transaction, routing and group
ordering, but no longer covers normal storage apply/flush waits. Selector parsing
and filesystem size accounting also happen outside it. Destination sharding and
full duplex multiplexing remain future architectural work, not claimed here.
Cloned messages share immutable `Arc<Body>` payloads, avoiding deep body copies
for virtual-topic fan-out, transactions and forwarding. Wire and serialized
storage representations are unchanged. Rust callers can keep using
`Message::new(destination, Body::...)`; direct body matching uses
`message.body.as_ref()`, replacement uses e.g. `Body::Empty.into()`, and intentional
copy-on-write mutation uses `Arc::make_mut(&mut message.body)`.

`GET /api/broker` includes cumulative `state_lock` acquisition/wait/hold/max-hold
nanoseconds and `storage` request/flush/serialized-byte/flush-time/group-size/
pending-byte/failure counters. `storage` is null for an in-memory broker.
The benchmark reports measured send p50/p95/p99 latency:

```sh
cargo run --release --example throughput -- memory 8 500 256
cargo run --release --example throughput -- sled 8 500 256 0
cargo run --release --example throughput -- sled 8 500 256 1000
```

Arguments are backend, producer threads, messages per producer, payload bytes,
and optional group delay in microseconds. The workload is bounded and uses
independent queues with no consumers; it measures native-core send completion,
not end-to-end protocol delivery, acknowledgement throughput or a Classic/Artemis
comparison. Temporary benchmark stores are removed after successful completion.

For orientation only, three alternating release-build trials in the development
environment (8 producers, 4,000 persistent 256-byte sends, fresh Sled stores)
gave median throughput of approximately **5,125 messages/s before this change**
versus **17,864 messages/s after**, with median per-run p99 send latency
approximately 33.1 ms versus 1.2 ms. The current runs used 771–842 flushes for
4,000 sends plus destination creation. These short, shared-environment results
are not production capacity estimates. Single-producer workloads can pay worker
handoff overhead without batching gains; benchmark your own workloads and disk.

## Known compatibility limits

- OpenWire negotiates v4–v12 with tight encoding and caches disabled. The
  ActiveMQ 6.2.6 Java client passes queue, selector, topic and transaction
  commit/rollback checks at each supported version, including a small producer
  window. Versions 1–3 are explicitly rejected. Lifecycle advisories preserve
  connection, producer, consumer, destination and removal payloads, but not
  every Classic command/advisory or transport option is implemented.
- STOMP `transformation:jms-map-json` SEND frames without `content-length`
  decode Classic typed map entries or plain scalar JSON objects into native
  map bodies. Invalid transformations return an ERROR frame; arbitrary object
  deserialization and XML transformations are unsupported. As in Classic,
  `content-length` selects a bytes message instead of applying the transformation.
- Repeated MQTT SUBSCRIBE replays matching retained messages, including when
  the subscription already exists. Same-QoS repeats preserve the consumer,
  durable queue and outstanding packet identifiers. MQTT's optional packet
  journal and core store are not an atomic persistence domain;
  a crash at that boundary can duplicate QoS 2 routing.
- Network connectors support duplex and non-conduit subscriptions; temporary
  queue proxies follow live demand and are deleted when that demand disappears.
  Queue/topic forwarding and temporary request/reply in both directions have
  been exercised against stock Classic 6.2.6. Temporary topic bridging remains
  unverified. Restart recovery after an established connection is covered by
  Rust integration tests and a stop/restart check against stock Classic 6.2.6;
  that is not exhaustive unclean-crash or persistent-peer recovery coverage.
  Advisory payload forwarding requires explicit static inclusion. Consumer
  advisory forwarding and dynamic advisory includes are rejected to prevent
  fabricated network demand; static temporary destinations are unsupported.
  Forwarding is not an atomic distributed transaction; loss of a remote
  response can cause duplicates on retry. An idle duplex consumer is recycled
  after 24 hours; this is not full Classic bridge/failover behavior.
- TLS/WebSocket transports, discovery/multicast, JMX, XPath selectors,
  retroactive consumers and MQTT 5 are unsupported.
- Complete Classic plugin, management, storage, clustering and failure-mode
  parity has not been established.

## Development

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

Third-party client checks live under `tests/interop/`. Follow each harness's
dependency/setup instructions. Core unit tests and protocol TCP tests run with
`cargo test`.

`tests/interop/openwire/run.sh` checks all supported versions (4–12) using an
unmodified ActiveMQ 6.2.6 Java client. Pass a version or inclusive version range
to narrow the run. `tests/interop/network/ClassicPeer.java` provides a stock
Classic peer and bidirectional queue/topic/temporary-reply checks; configure
duplex demand includes for `queue://interop.>`, `topic://interop.>` and
`temp-queue://>` when running that harness.

To check STOMP map JSON against an unmodified Java JMS client, set
`AMQ_CLASSPATH` to the ActiveMQ 6.2.6 client and its runtime dependency jars,
enable both listeners, and run:

```sh
java --class-path "$AMQ_CLASSPATH" tests/interop/stomp/MapJsonCheck.java \
  tcp://127.0.0.1:61616 61613
```
