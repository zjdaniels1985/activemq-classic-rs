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
  exclusions, remote authentication, URI failover and broker-path/TTL filtering.
- An embedded Rust/Axum administrative web console and JSON API.
- Configurable structured logging with text/JSON output and optional log files.

OpenWire interoperability has been exercised with the ActiveMQ 6.2.6 Java
client, including topics, transactions, durable subscriptions, selectors,
temporary destinations, scheduling and XA. STOMP and MQTT interoperability
harnesses use `stomp.py` and `paho-mqtt`. AMQP checks use Qpid Proton, including
transactional send commit/rollback and durable subscription reconnect with
offline delivery. These checks do not establish complete Classic behavioral
parity.

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

## Known compatibility limits

- OpenWire negotiation advertises up to version 12, but v1–v11 layouts are
  not independently verified. Not all Classic commands/advisories are covered.
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
- Network connectors require separate reverse connectors instead of duplex.
  Non-conduit subscriptions, temporary-destination forwarding and advisory
  payload forwarding are unsupported. Established-connection restart recovery
  has not been independently verified. Forwarding is not an atomic distributed
  transaction; loss of a remote response can cause duplicates on retry.
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

To check STOMP map JSON against an unmodified Java JMS client, set
`AMQ_CLASSPATH` to the ActiveMQ 6.2.6 client and its runtime dependency jars,
enable both listeners, and run:

```sh
java --class-path "$AMQ_CLASSPATH" tests/interop/stomp/MapJsonCheck.java \
  tcp://127.0.0.1:61616 61613
```
