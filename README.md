# activemq-classic-rs

A Rust broker prototype targeting Apache ActiveMQ Classic compatibility. The
goal is to preserve existing Java-client interoperability through compatible
wire protocols while providing Rust-native APIs instead of translating the
Java-only JMS API.

## Current status

This is an early, incomplete implementation and is **not** a drop-in replacement
for ActiveMQ Classic. The current network path implements a limited OpenWire
queue workflow (connection setup, queue producers/consumers, dispatch, and
acknowledgements). Persistent messages are stored in Sled and recovered after a
restart. A basic Rust-built web console lists queue depths and purges queues.
Structured logs use `tracing`. The limited queue send/receive path has been
smoke-tested with the ActiveMQ 6.2.6 Java client; this does not imply full
OpenWire compatibility.

Topics, selectors, transactions, security, scheduled delivery, temporary
destinations, broker networking, and AMQP, MQTT, and STOMP adapters are not
implemented. Enabling one of those protocol listeners is rejected at startup;
the configuration sections reserve room for those future adapters. The broker
has not achieved full ActiveMQ Classic feature or behavioral parity and should
not be exposed to untrusted networks.

## Build and run

Requires Rust 1.85 or newer.

```sh
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo run
```

By default the executable reads `./activemq-rust.toml` when it exists and
otherwise uses built-in defaults. Override it with `--config PATH`. OpenWire
defaults to `127.0.0.1:61616`, Sled data to `./data/sled`, and the web console to
`http://127.0.0.1:8161/`. For example:

```sh
cargo run -- --config activemq-rust.toml
cargo run -- --bind 127.0.0.1:61617 --data-dir ./data/test
```

Set `RUST_LOG` to control structured log levels, for example
`RUST_LOG=debug cargo run`.

## Configuration

`activemq-rust.toml` contains broker identity and frame-size limits, individual
protocol listener settings, the persistence backend/path, and web-console
settings. Keep unimplemented protocols disabled; the broker fails clearly
rather than silently ignoring an enabled listener. Sled is currently the only
supported persistence backend.

The web console supports queue listing and queue purge. Binding it to a
non-loopback address requires a non-empty `web_console.admin_token`; the
browser console will prompt for that bearer token. Remote console traffic is
plain HTTP, so use a trusted network or a TLS-terminating reverse proxy.

## Compatibility roadmap

1. Expand OpenWire compatibility and test against supported ActiveMQ Java clients.
2. Add Classic queue/topic semantics, transactions, selectors, security,
   redelivery policies, scheduling, and temporary destinations.
3. Implement the configured protocol adapters and operational features.
4. Continue compatibility and behavioral testing against ActiveMQ Classic.

The roadmap describes planned work, not capabilities of the current broker.
