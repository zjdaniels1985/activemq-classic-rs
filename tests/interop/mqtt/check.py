#!/usr/bin/env python3
"""Real paho-mqtt 2.1.0 checks. Run from any directory after cargo build.

Install: python -m venv target/mqtt-venv
         target/mqtt-venv/bin/pip install paho-mqtt==2.1.0
Run:     target/mqtt-venv/bin/python tests/interop/mqtt/check.py
All broker files are created below target/tmp; ports stay in 64000-64999.
"""

import json
import os
from pathlib import Path
import queue
import random
import shutil
import signal
import socket
import subprocess
import time
import threading
import urllib.request

import paho.mqtt
import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[3]
WORK = ROOT / "target" / "tmp" / f"mqtt-interop-{os.getpid()}"


def wait(predicate, description, seconds=10):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(0.03)
    raise AssertionError(f"timeout: {description}")


def free_port(exclude=()):
    for port in random.sample(range(64000, 65000), 1000):
        if port in exclude:
            continue
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                pass
    raise RuntimeError("no MQTT test ports free")


class Broker:
    def __init__(self):
        WORK.mkdir(parents=True)
        self.port = free_port()
        self.console = free_port((self.port,))
        self.process = None
        self.log = (WORK / "broker.log").open("w+")
        self.config = WORK / "broker.toml"
        self.config.write_text(f'''
[protocols.openwire]
enabled = false
[protocols.stomp]
enabled = false
[protocols.amqp]
enabled = false
[protocols.mqtt]
enabled = true
bind = "127.0.0.1:{self.port}"
[protocols.mqtt.options]
session_state_dir = "{WORK / 'sessions'}"
[web_console]
bind = "127.0.0.1:{self.console}"
[persistence]
backend = "sled"
path = "{WORK / 'sled'}"
[security]
enabled = true
[[security.users]]
username = "mqtt"
password = "secret"
groups = ["users"]
[policy]
advisory_support = false
[logging]
level = "warn"
''')

    def start(self):
        self.process = subprocess.Popen(
            ["cargo", "run", "--quiet", "--", "--config", str(self.config)],
            cwd=ROOT, stdout=self.log, stderr=subprocess.STDOUT,
        )
        def ready():
            if self.process.poll() is not None:
                raise AssertionError("broker exited before startup")
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.console}/api/broker", timeout=0.3) as response:
                    return response.status == 200
            except OSError:
                return False
        wait(ready, "broker HTTP readiness", 120)

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                raise AssertionError("broker did not stop gracefully")
        self.process = None

    def send(self, destination, body):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.console}/api/destinations/topic/{destination}/send",
            data=json.dumps({"body": body}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 200


class Client:
    all = []

    def __init__(self, broker, name, clean=True, password="secret", version=mqtt.MQTTv311, will=None, keepalive=2):
        self.messages = queue.Queue()
        self.subacks = queue.Queue()
        self.unsubacks = queue.Queue()
        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.reason = None
        self.present = None
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, name, clean_session=clean,
                                  protocol=version, reconnect_on_failure=False)
        self.client.username_pw_set("mqtt", password)
        if will:
            self.client.will_set(*will)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = lambda *_: self.disconnected.set()
        self.client.on_message = lambda _, __, message: self.messages.put(message)
        self.client.on_subscribe = lambda _, __, mid, codes, props: self.subacks.put((mid, codes))
        self.client.on_unsubscribe = lambda *_: self.unsubacks.put(True)
        self.all.append(self)
        self.client.connect("127.0.0.1", broker.port, keepalive=keepalive)
        self.client.loop_start()
        assert self.connected.wait(5), f"no CONNACK for {name}"
        if password == "secret":
            assert self.reason == 0, self.reason

    def on_connect(self, client, userdata, flags, reason, properties):
        self.reason = reason.value
        self.present = flags.session_present
        self.connected.set()

    def subscribe(self, topic, qos):
        result, mid = self.client.subscribe(topic, qos)
        assert result == mqtt.MQTT_ERR_SUCCESS
        ack_mid, codes = self.subacks.get(timeout=5)
        assert ack_mid == mid and codes[0].value == qos, (ack_mid, codes)

    def publish(self, topic, body, qos=0, retain=False):
        info = self.client.publish(topic, body, qos, retain)
        info.wait_for_publish(timeout=5)
        assert info.is_published(), f"publish did not complete: {topic}, QoS {qos}"

    def receive(self, topic, body, qos=None, retain=None):
        message = self.messages.get(timeout=5)
        assert (message.topic, message.payload) == (topic, body), (message.topic, message.payload)
        if qos is not None:
            assert message.qos == qos, message.qos
        if retain is not None:
            assert message.retain == retain, message.retain
        return message

    def empty(self):
        try:
            message = self.messages.get(timeout=0.25)
        except queue.Empty:
            return
        raise AssertionError(f"unexpected message: {message.topic} {message.payload}")

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()

    def abort(self):
        # Abrupt TCP loss, with no MQTT DISCONNECT.
        self.client._sock_close()
        self.client.loop_stop()


def checks(broker):
    refused = Client(broker, "bad-auth", password="wrong")
    assert refused.reason != 0
    refused.close()
    v31 = Client(broker, "mqtt-v31", version=mqtt.MQTTv31)
    v31.close()
    pub = Client(broker, "publisher")
    sub = Client(broker, "subscriber")
    sub.subscribe("qos/+", 2)
    for qos in (0, 1, 2):
        pub.publish(f"qos/{qos}", f"payload-{qos}".encode(), qos)
        sub.receive(f"qos/{qos}", f"payload-{qos}".encode(), qos)
    sub.subscribe("deep/#", 1)
    pub.publish("deep/a/b", b"wildcard", 2)
    sub.receive("deep/a/b", b"wildcard", 1)
    pub.publish("retained/value", b"saved", 1, True)
    retained = Client(broker, "retained-reader")
    retained.subscribe("retained/#", 2)
    retained.receive("retained/value", b"saved", 1, True)
    retained.subscribe("retained/#", 2)
    replay = retained.receive("retained/value", b"saved", 1, True)
    assert not replay.dup, "repeated SUBSCRIBE must create a new retained delivery"
    retained.close()
    pub.publish("retained/value", b"", 1, True)
    cleared = Client(broker, "retained-cleared")
    cleared.subscribe("retained/#", 2)
    cleared.empty()
    cleared.close()
    sub.subscribe("will/#", 2)
    doomed = Client(broker, "doomed", will=("will/crash", b"gone", 1, True))
    doomed.abort()
    sub.receive("will/crash", b"gone", 1, False)
    will_reader = Client(broker, "will-retained")
    will_reader.subscribe("will/crash", 1)
    will_reader.receive("will/crash", b"gone", 1, True)
    will_reader.close()
    graceful = Client(broker, "graceful", will=("will/graceful", b"bad", 1, False))
    graceful.close()
    sub.empty()
    durable = Client(broker, "durable", clean=False)
    assert not durable.present
    durable.subscribe("offline/#", 2)
    durable.close()
    pub.publish("offline/first", b"queued", 1)
    durable = Client(broker, "durable", clean=False)
    assert durable.present
    durable.receive("offline/first", b"queued", 1)
    durable.close()
    old = Client(broker, "takeover")
    replacement = Client(broker, "takeover")
    assert old.disconnected.wait(5), "old client was not disconnected"
    old.close()
    replacement.publish("takeover/check", b"alive", 1)
    replacement.close()
    sub.subscribe("a/b", 1)
    broker.send("a.b", "cross-protocol")
    sub.receive("a/b", b"cross-protocol", 0)
    sub.client.unsubscribe("a/b")
    assert sub.unsubacks.get(timeout=5)
    broker.send("a.b", "not-subscribed")
    sub.empty()
    # Let paho's network loop send keepalive PINGREQs over >1.5 keepalive.
    time.sleep(3.5)
    assert not sub.disconnected.is_set(), "keepalive pings did not keep connection open"
    pub.publish("qos/1", b"after-ping", 1)
    sub.receive("qos/1", b"after-ping", 1)
    pub.publish("offline/restart", b"persistent", 2)
    pub.publish("restart/retained", b"retained-restart", 1, True)
    sub.close()
    pub.close()
    broker.stop()
    broker.start()
    durable = Client(broker, "durable", clean=False)
    assert durable.present
    durable.receive("offline/restart", b"persistent", 2)
    durable.close()
    retained = Client(broker, "restart-retained")
    retained.subscribe("restart/retained", 1)
    retained.receive("restart/retained", b"retained-restart", 1, True)
    retained.close()
    reset = Client(broker, "durable", clean=True)
    assert not reset.present
    reset.close()


def main():
    assert paho.mqtt.__version__ == "2.1.0", paho.mqtt.__version__
    broker = Broker()
    success = False
    try:
        broker.start()
        checks(broker)
        success = True
        print("PASS paho-mqtt 2.1.0: v3.1/v3.1.1, auth, QoS 0/1/2, wildcards, retain/clear, will, offline/restart persistence, pings, takeover, REST cross-protocol")
    finally:
        for client in Client.all:
            client.close()
        broker.stop()
        broker.log.close()
        if not success:
            print((WORK / "broker.log").read_text())
        else:
            shutil.rmtree(WORK)


if __name__ == "__main__":
    main()
