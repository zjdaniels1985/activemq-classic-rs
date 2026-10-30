# MQTT transport

Implements MQTT 3.1 (`MQIsdp`, level 3) and MQTT 3.1.1 (`MQTT`, level 4)
over TCP. Authentication and topic authorization use the broker core. TLS,
WebSockets, MQTT 5, and the virtual-topic subscription strategy are not provided.

```toml
[protocols.mqtt]
enabled = true
bind = "127.0.0.1:1883"

[protocols.mqtt.options]
prefetch = 1000
connect_timeout_ms = 10000
session_state_dir = "data/mqtt-sessions"

[persistence]
backend = "sled"
path = "data/sled"
```

`broker.max_frame_size` bounds complete inbound and outbound packets. The
remaining-length field is limited to four bytes. A client with a nonzero
keepalive is disconnected after 1.5 times that interval without a complete
packet. Zero disables this timeout. The CONNECT timeout is independent.

## ActiveMQ compatibility

Topic conversion follows `MQTTProtocolSupport` exactly: `/` and `.`, `+` and
`*`, and `#` and `>` are swapped in both directions. MQTT publishes use core
topics. Cross-protocol payloads use `Body::to_bytes()` (text is UTF-8).
Root wildcards do not deliver topics beginning with `$`; explicit `$` filters
work. Broker advisory objects are not exposed as application MQTT messages.

Inbound publishes carry integer property `ActiveMQ.MQTT.QoS`. Outbound QoS is
the smaller of that value and the subscription QoS. For messages without that
property, the Classic converter's fallback is preserved: persistent messages
use QoS 0, nonpersistent messages use QoS 1. Other protocols can set the property
to select QoS explicitly. Retained publishes use core `ActiveMQ.Retain`;
retained replays are identified by `ActiveMQ.Retained`. Empty retained payloads
clear the retained entry, including its persisted copy.

Clean sessions remove previous durable subscriptions belonging to the client
ID. Persistent-session QoS 1/2 subscriptions are core durable subscriptions,
named `AT_LEAST_ONCE:<filter>` or `EXACTLY_ONCE:<filter>`, as with Classic's
default strategy. Core Sled persistence retains these subscriptions and queued
persistent messages across restarts without adapter-specific configuration.
QoS 0 subscriptions are remembered for reconnects but are not core durables,
matching Classic's default strategy.

`session_state_dir` is optional, but recommended with Sled: it also journals
empty sessions, QoS 0 subscription metadata, packet IDs, inbound QoS 2 payloads,
and outbound QoS 2 handshake phases across broker restarts. Without it, that
wire-level state survives TCP reconnects only. Files are atomically replaced,
use hashed client IDs as filenames, and never contain credentials. Give each
broker its own directory, and back it up together with its core store.

The adapter journal and core store are separate persistence domains: there is
no atomic transaction between routing a QoS 2 publish and updating its journal.
A process failure at that boundary can cause duplicate routing. Normal
reconnects and orderly broker restarts preserve the handshake. Repeating an
existing SUBSCRIBE replays the matching retained snapshot. Same-QoS repeats
keep the current subscription and in-flight packet state; each replay gets
fresh delivery and packet identifiers rather than masquerading as a retransmit.

## Verification

```sh
cargo test --test mqtt_protocol
python -m venv target/mqtt-venv
mkdir -p target/tmp
TMPDIR="$PWD/target/tmp" target/mqtt-venv/bin/pip install paho-mqtt==2.1.0
target/mqtt-venv/bin/python tests/interop/mqtt/check.py
```

The Python harness starts and restarts `cargo run -- --config ...` with only
MQTT and the console enabled, using random ports in 64000–64999. Its config,
Sled database, packet journal, and logs stay under `target/tmp`. It checks both
protocol versions, authentication rejection, QoS 0/1/2, wildcard subscriptions,
retained set/replay/clear, retained wills, graceful will suppression, persistent
offline/restart delivery, ping keepalive, takeover, unsubscribe, clean-session
reset, and REST-topic-to-MQTT delivery. Rust tests additionally exercise frame
bounds, fragmented packets interleaved with core events, duplicate QoS 2
packets, persistent retransmit IDs/DUP, and both QoS 2 handshake restart phases.
