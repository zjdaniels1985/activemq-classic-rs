# activemq-classic-rs

A Rust broker implementation targeting Apache ActiveMQ Classic compatibility.
The goal is to preserve existing client interoperability through ActiveMQ's wire
protocols, while exposing idiomatic Rust APIs rather than attempting to recreate
the Java-only JMS API.

## Status

This repository is an early prototype, not yet an ActiveMQ Classic-compatible
broker. It contains a thread-safe in-memory FIFO queue core, bounded OpenWire
framing, and a TCP listener that negotiates OpenWire and reports broker identity.
The listener acknowledges selected setup commands but does not yet implement
message production/consumption, topics, persistence, transactions, security, or
the rest of ActiveMQ Classic's broker semantics. Do not use it as a drop-in
replacement or expose it to untrusted networks.

## Build and test

Requires Rust 1.85 or newer.

```sh
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo run -- --bind 127.0.0.1:61616
```

The listener defaults to `127.0.0.1:61616`. It currently supports only the
OpenWire negotiation and broker-identification handshake; a successful client
connection does not mean queue operations are supported.

## Compatibility roadmap

1. Establish a runnable TCP listener and OpenWire negotiation.
2. Implement OpenWire connection/session, producer, consumer, message dispatch,
   acknowledgement, and queue semantics with ActiveMQ Java-client tests.
3. Add durable storage and recovery, then transactions, security, topics,
   scheduling, and broker semantics.
4. Add the remaining protocols and operational integrations as separately
   testable adapters.

The frame codec handles OpenWire's length-prefixed frame boundary. Command
payload support is limited to the initial negotiation and broker-identification
exchange.
