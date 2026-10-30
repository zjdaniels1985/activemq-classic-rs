#!/usr/bin/env python3
"""Exercise the STOMP adapter with the real stomp.py 9.0.0 client.

Run against a broker listening on 127.0.0.1:61613, optionally passing HOST PORT.
Set STOMP_USER and STOMP_PASSWORD to validate an enabled authentication config.
"""
import os
import json
import sys
import threading
import time
import uuid

import stomp

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 61613


class Listener(stomp.ConnectionListener):
    def __init__(self):
        self.messages = []
        self.receipts = []
        self.errors = []
        self.changed = threading.Condition()

    def on_message(self, frame):
        with self.changed:
            self.messages.append(frame)
            self.changed.notify_all()

    def on_receipt(self, frame):
        with self.changed:
            self.receipts.append(frame)
            self.changed.notify_all()

    def on_error(self, frame):
        with self.changed:
            self.errors.append(frame)
            self.changed.notify_all()

    def wait_message(self, count=1, timeout=5):
        deadline = time.monotonic() + timeout
        with self.changed:
            while len(self.messages) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(f"timed out waiting for {count} messages; got {len(self.messages)}")
                self.changed.wait(remaining)
            return self.messages[count - 1]

    def wait_body(self, body, timeout=5):
        deadline = time.monotonic() + timeout
        with self.changed:
            while True:
                for message in self.messages:
                    if message.body == body:
                        return message
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(f"timed out waiting for body {body!r}")
                self.changed.wait(remaining)

    def wait_receipt(self, receipt_id, timeout=5):
        deadline = time.monotonic() + timeout
        with self.changed:
            while not any(frame.headers.get("receipt-id") == receipt_id for frame in self.receipts):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(f"timed out waiting for receipt {receipt_id}")
                self.changed.wait(remaining)


def client(client_id=None, heartbeats=(0, 0), auto_content_length=True):
    conn = stomp.Connection12(
        [(HOST, PORT)], heartbeats=heartbeats, timeout=3,
        auto_content_length=auto_content_length,
    )
    listener = Listener()
    conn.set_listener("check", listener)
    headers = {"client-id": client_id} if client_id else {}
    conn.connect(login=os.getenv("STOMP_USER", ""), passcode=os.getenv("STOMP_PASSWORD", ""), wait=True, headers=headers)
    return conn, listener


def main():
    suffix = uuid.uuid4().hex[:8]
    conn, listener = client(heartbeats=(500, 500))
    queue = f"/queue/interop.{suffix}"
    conn.subscribe(queue, id="q", ack="auto")
    conn.send(queue, "queue-ok", headers={"receipt": "send-q"})
    assert listener.wait_message().body == "queue-ok"
    listener.wait_receipt("send-q")

    topic = f"/topic/interop.{suffix}"
    conn.subscribe(topic, id="topic", ack="auto")
    conn.send(topic, "topic-ok")
    assert listener.wait_message(2).body == "topic-ok"

    selector_queue = f"/queue/selector.{suffix}"
    conn.subscribe(selector_queue, id="selector", ack="auto", headers={"selector": "color = 'blue'"})
    conn.send(selector_queue, "skip", headers={"color": "red"})
    conn.send(selector_queue, "selected", headers={"color": "blue"})
    assert listener.wait_message(3).body == "selected"

    ack_queue = f"/queue/ack.{suffix}"
    conn.subscribe(ack_queue, id="ack", ack="client-individual")
    conn.send(ack_queue, "redeliver-me")
    first = listener.wait_message(4)
    conn.nack(first.headers["ack"])
    second = listener.wait_message(5)
    assert second.body == "redeliver-me" and second.headers.get("redelivered", "false") == "true"
    conn.ack(second.headers["ack"])

    cumulative_queue = f"/queue/cumulative.{suffix}"
    conn.subscribe(cumulative_queue, id="cumulative", ack="client")
    conn.send(cumulative_queue, "cumulative-first")
    conn.send(cumulative_queue, "cumulative-second")
    first_cumulative = listener.wait_body("cumulative-first")
    second_cumulative = listener.wait_body("cumulative-second")
    conn.ack(second_cumulative.headers["ack"])
    time.sleep(0.2)
    assert listener.messages.count(first_cumulative) == 1

    tx_queue = f"/queue/tx.{suffix}"
    conn.subscribe(tx_queue, id="tx", ack="auto")
    conn.begin("commit-tx")
    conn.send(tx_queue, "committed", headers={"transaction": "commit-tx"})
    conn.commit("commit-tx")
    assert listener.wait_body("committed").body == "committed"
    conn.begin("abort-tx")
    conn.send(tx_queue, "aborted", headers={"transaction": "abort-tx"})
    conn.abort("abort-tx")
    time.sleep(0.2)
    assert all(frame.body != "aborted" for frame in listener.messages)

    client_id = f"durable-{suffix}"
    durable, durable_listener = client(client_id=client_id)
    durable_topic = f"/topic/durable.{suffix}"
    durable_headers = {"activemq.subscriptionName": f"sub-{suffix}"}
    durable.subscribe(durable_topic, id="durable", ack="auto", headers=durable_headers)
    durable.disconnect(receipt="durable-offline")
    conn.send(durable_topic, "durable-ok")
    durable, durable_listener = client(client_id=client_id)
    durable.subscribe(durable_topic, id="durable", ack="auto", headers=durable_headers)
    assert durable_listener.wait_body("durable-ok").body == "durable-ok"
    durable.unsubscribe(id="durable", headers=durable_headers)
    durable.disconnect(receipt="durable-offline")

    temp = f"/temp-queue/reply-{suffix}"
    conn.subscribe(temp, id="temp", ack="auto")
    responder, responder_listener = client()
    responder.send(f"/remote-temp-queue/reply-{suffix}", "reply-ok")
    assert listener.wait_body("reply-ok").body == "reply-ok"
    responder.disconnect()
    responder2, _ = client()
    responder2.send(f"/remote-temp-queue/reply-{suffix}", "reply-again")
    assert listener.wait_body("reply-again").body == "reply-again"
    responder2.disconnect()

    browser_queue = f"/queue/browser.{suffix}"
    conn.send(browser_queue, "browse-me")
    conn.subscribe(browser_queue, id="browser", ack="auto", headers={"browser": "true"})
    browser = listener.wait_body("browse-me")
    assert browser.body == "browse-me"
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and not any(m.headers.get("browser:end") == "true" for m in listener.messages):
        time.sleep(0.05)
    assert any(m.headers.get("browser:end") == "true" for m in listener.messages), [m.headers for m in listener.messages]

    delayed = f"/queue/delay.{suffix}"
    conn.subscribe(delayed, id="delay", ack="auto")
    conn.send(delayed, "scheduled", headers={"AMQ_SCHEDULED_DELAY": "300"})
    assert listener.wait_body("scheduled", timeout=4).body == "scheduled"
    time.sleep(1.2)
    assert conn.is_connected(), "negotiated 1.2 heartbeats did not keep the connection alive"

    conn11 = stomp.Connection11([(HOST, PORT)], heartbeats=(500, 500), timeout=3, vhost=HOST)
    listener11 = Listener()
    conn11.set_listener("check-11", listener11)
    conn11.connect(login=os.getenv("STOMP_USER", ""), passcode=os.getenv("STOMP_PASSWORD", ""), wait=True)
    conn11.subscribe(f"/queue/stomp11.{suffix}", id="stomp11", ack="auto")
    conn11.send(f"/queue/stomp11.{suffix}", "stomp-11-ok")
    assert listener11.wait_body("stomp-11-ok").body == "stomp-11-ok"
    time.sleep(1.2)
    assert conn11.is_connected(), "negotiated 1.1 heartbeats did not keep the connection alive"
    conn11.disconnect(receipt="goodbye-11")
    conn.disconnect()

    maps, map_listener = client(auto_content_length=False)
    map_queue = f"/queue/maps.{suffix}"
    maps.subscribe(map_queue, id="maps", ack="auto")
    maps.send(
        map_queue,
        json.dumps({"map": {"entry": [
            {"string": ["name", "order"]},
            {"string": "count", "int": 42},
            {"string": "enabled", "boolean": True},
        ]}}),
        headers={"transformation": "jms-map-json"},
    )
    transformed = map_listener.wait_message()
    assert json.loads(transformed.body) == {
        "name": "order", "count": 42, "enabled": True,
    }, transformed.body
    maps.disconnect()

    if os.getenv("STOMP_USER"):
        bad = stomp.Connection12([(HOST, PORT)], timeout=2)
        failed = Listener()
        bad.set_listener("bad", failed)
        try:
            bad.connect(login=os.getenv("STOMP_USER"), passcode="incorrect", wait=True)
        except Exception:
            pass
        else:
            raise AssertionError("invalid STOMP credentials unexpectedly connected")
        finally:
            bad.disconnect()

    print("stomp.py 9.0.0 interoperability scenarios passed")


if __name__ == "__main__":
    main()
