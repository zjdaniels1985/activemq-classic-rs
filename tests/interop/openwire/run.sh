#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$repo_root"
if [[ -n "${OPENWIRE_INTEROP_PORT:-}" ]]; then
  port="$OPENWIRE_INTEROP_PORT"
else
  port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
fi
java_dir="tests/interop/openwire"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$java_dir/target" "$java_dir/classpath.txt"
}
trap cleanup EXIT

export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=2
cargo build --bin activemq-classic-rs
(cd "$java_dir" && mvn -q package dependency:build-classpath \
  -Dmdep.outputFile=classpath.txt)

target/debug/activemq-classic-rs \
  --config "$java_dir/config.toml" --bind "127.0.0.1:$port" --in-memory \
  >"${OPENWIRE_INTEROP_BROKER_LOG:-/dev/null}" 2>&1 &
server_pid=$!

for _ in $(seq 1 100); do
  if python3 -c "import socket; s=socket.create_connection(('127.0.0.1', $port), timeout=.2); s.close()" \
    2>/dev/null; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    echo "OpenWire broker exited before becoming ready" >&2
    exit 1
  fi
  sleep .1
done

OPENWIRE_INTEROP_URL="tcp://127.0.0.1:$port" \
  java -cp "$java_dir/target/classes:$(cat "$java_dir/classpath.txt")" InteropCheck "$@"
