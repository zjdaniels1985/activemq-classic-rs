#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."

export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=2
cargo build --bin activemq-classic-rs
mkdir -p target/tmp
work="$(mktemp -d "$PWD/target/tmp/classic-parity.XXXXXX")"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

mvn -q -f tests/interop/network/pom.xml dependency:build-classpath \
  -Dmdep.outputFile="$work/classpath.txt"
classpath="$(cat "$work/classpath.txt")"
source_file="tests/interop/openwire/ClassicParityCheck.java"
for always_retroactive in false true; do
for policy in none last_image fixed_count; do
  port="$(python3 - "$work/config.toml" "$policy" "$always_retroactive" <<'PY'
import pathlib
import socket
import sys

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
policy = sys.argv[2]
configuration = f"""
[policy]
always_retroactive = {sys.argv[3]}
[policy.subscription_recovery]
type = "{policy}"
"""
if policy == "fixed_count":
    configuration += "maximum_size = 2\n"
configuration += f"""
[protocols.openwire]
enabled = true
bind = "127.0.0.1:{port}"
[protocols.stomp]
enabled = false
[protocols.amqp]
enabled = false
[protocols.mqtt]
enabled = false
[web_console]
enabled = false
[persistence]
backend = "memory"
[logging]
level = "warn"
"""
pathlib.Path(sys.argv[1]).write_text(configuration)
print(port)
PY
)"
  target/debug/activemq-classic-rs --config "$work/config.toml" \
    >"$work/$policy-$always_retroactive.log" 2>&1 &
  server_pid=$!
  ready=false
  for _ in $(seq 1 100); do
    if python3 -c "import socket; socket.create_connection(('127.0.0.1',$port), .2).close()" 2>/dev/null; then
      ready=true
      break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      cat "$work/$policy-$always_retroactive.log" >&2
      exit 1
    fi
    sleep .1
  done
  if [[ "$ready" != true ]]; then
    echo "Rust broker did not become ready; logs: $work" >&2
    exit 1
  fi
  versions="${OPENWIRE_PARITY_VERSIONS:-1 3 12}"
  for version in $versions; do
    java -cp "$classpath" "$source_file" reference "$policy" "$version" "$always_retroactive"
    java -cp "$classpath" "$source_file" "tcp://127.0.0.1:$port" "$policy" "$version" "$always_retroactive"
  done
  cleanup
  server_pid=""
done
done
echo "PASS Classic 6.2.6 differential parity checks; logs: $work"
