# activemq-classic-rs

A Rust broker implementation targeting Apache ActiveMQ Classic compatibility.
The goal is to preserve existing client interoperability through ActiveMQ's wire
protocols, while exposing idiomatic Rust APIs rather than attempting to recreate
the Java-only JMS API.

## Status

This repository is at the foundation stage, not yet a usable or wire-compatible
broker. The initial implementation contains a thread-safe in-memory FIFO queue
core and a bounded OpenWire frame reader/writer. OpenWire command marshalling,
connection negotiation, TCP transport, persistence, and protocol-level
interoperability are not implemented yet.

## Build and test

Requires Rust 1.85 or newer.

```sh
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

## Compatibility roadmap

1. Define wire-format and broker-behavior compatibility tests against Apache
   ActiveMQ Classic and its existing Java clients.
2. Implement OpenWire negotiation and a minimal queue path, including
   connection, producer, consumer, message, and acknowledgement commands.
3. Add durable storage and recovery, then transactions and broker semantics.
4. Add AMQP, MQTT, STOMP, and operational integrations as separately testable
   protocol adapters.

The frame codec handles only OpenWire's length-prefixed frame boundary; it does
not decode or encode OpenWire command payloads.
