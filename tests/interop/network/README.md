# Stock Classic network interoperability

Requires a JDK supporting Jakarta JMS, Maven, and the Rust toolchain.
The peer runs unmodified ActiveMQ Classic 6.2.6 in memory.

From the repository root:

```sh
mkdir -p target/tmp/classic-peer-java
mvn -q -f tests/interop/network/pom.xml dependency:build-classpath \
  -Dmdep.outputFile="$PWD/target/tmp/classic-peer-classpath.txt"
javac -cp "$(cat target/tmp/classic-peer-classpath.txt)" \
  -d target/tmp/classic-peer-java tests/interop/network/ClassicPeer.java
java -cp "target/tmp/classic-peer-java:$(cat target/tmp/classic-peer-classpath.txt)" \
  ClassicPeer serve 63143 target/tmp/classic-peer-ready
```

In another terminal, start the Rust broker with a memory backend, OpenWire
bound to `127.0.0.1:63142`, and this connector:

```toml
[[network_connectors]]
name = "stock-classic"
uris = ["tcp://127.0.0.1:63143"]
duplex = true
network_ttl = 2
dynamically_included_destinations = [
  "queue://interop.>",
  "topic://interop.>",
  "temp-queue://>",
]
reconnect_delay_ms = 100
```

After both listeners are ready, run:

```sh
java -cp "target/tmp/classic-peer-java:$(cat target/tmp/classic-peer-classpath.txt)" \
  ClassicPeer check tcp://127.0.0.1:63142 tcp://127.0.0.1:63143
```

Each run uses unique destinations and checks queue and topic forwarding plus
temporary queue request/reply in both directions. It also checks for immediate
duplicates; this does not establish exactly-once delivery under failure.
Stop both test brokers after the check. Rust integration tests separately
exercise established-connection restart recovery, non-conduit subscriptions,
advisory payloads, exclusions, TTL and temporary proxy cleanup.
