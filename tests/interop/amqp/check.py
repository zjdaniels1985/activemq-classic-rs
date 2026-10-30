#!/usr/bin/env python3
"""Qpid Proton 0.41.1 interop checks; expects an AMQP 1.0 broker at AMQP_URL."""

import os
import sys
import time

from proton import Delivery, Message, ModifiedDisposition, symbol
from proton.handlers import MessagingHandler
from proton.reactor import Container, DurableSubscription, Selector
from proton.utils import BlockingConnection

URL = os.environ.get("AMQP_URL", "amqp://127.0.0.1:65042")
TIMEOUT = 6


class CloseConnection:
    def __init__(self, connection):
        self.connection = connection

    def on_timer_task(self, event):
        self.connection.close()
        event.container.stop()


class RejectProbe(MessagingHandler):
    def __init__(self, address, body, *, source):
        super().__init__(prefetch=10, auto_accept=False)
        self.address = address
        self.body = body
        self.source = source
        self.connection = None
        self.sender = None
        self.sent = False
        self.received = False
        self.failure = None

    def on_start(self, event):
        self.connection = event.container.connect(URL)
        if self.source:
            self.sender = event.container.create_sender(self.connection, self.address)
            event.container.create_receiver(self.connection, self.address)
        else:
            event.container.create_receiver(self.connection, self.address)
        event.reactor.schedule(TIMEOUT, self)

    def on_sendable(self, event):
        if self.source and not self.sent:
            self.sent = True
            event.sender.send(Message(body=self.body, durable=True))

    def on_message(self, event):
        try:
            assert event.message.body == self.body, event.message.body
            event.delivery.update(Delivery.REJECTED if self.source else Delivery.ACCEPTED)
            event.delivery.settle()
            self.received = True
            event.reactor.schedule(0.1, CloseConnection(self.connection))
        except Exception as error:
            self.failure = error
            self.connection.close()

    def on_timer_task(self, event):
        if not self.received:
            self.failure = TimeoutError(f"timed out receiving from {self.address}")
        self.connection.close()
        event.container.stop()


def run_reject_to_dlq(address, body):
    rejected = RejectProbe(address, body, source=True)
    Container(rejected).run()
    if rejected.failure:
        raise rejected.failure
    if not rejected.received:
        raise AssertionError("source message was not rejected")
    time.sleep(0.25)

    dead_letter = RejectProbe("queue://ActiveMQ.DLQ", body, source=False)
    Container(dead_letter).run()
    if dead_letter.failure:
        raise dead_letter.failure
    if not dead_letter.received:
        raise AssertionError("rejected message did not reach ActiveMQ.DLQ")
    time.sleep(0.25)
    print("PASS rejected delivery to DLQ", flush=True)


class RoundTrip(MessagingHandler):
    def __init__(self, address, messages, expected, *, selector=None, dynamic=False, outcome="accepted"):
        super().__init__(prefetch=10, auto_accept=False)
        self.address = address
        self.messages = messages
        self.expected = expected
        self.selector = selector
        self.dynamic = dynamic
        self.outcome = outcome
        self.connection = None
        self.sender = None
        self.receiver = None
        self.sent = False
        self.failure = None
        self.received = []
        self.finished = False

    def on_start(self, event):
        self.connection = event.container.connect(URL)
        if not self.dynamic:
            self.sender = event.container.create_sender(
                self.connection, self.address
            )
        self.receiver = event.container.create_receiver(
            self.connection,
            None if self.dynamic else self.address,
            dynamic=self.dynamic,
            options=[Selector(self.selector)] if self.selector else None,
        )
        event.reactor.schedule(TIMEOUT, self)

    def on_link_opened(self, event):
        if self.dynamic and not event.link.is_sender:
            address = event.link.remote_source.address
            if not address:
                self.failure = AssertionError("broker did not assign dynamic address")
                self.connection.close()
                return
            self.sender = event.container.create_sender(
                self.connection, address
            )

    def on_sendable(self, event):
        if self.sender is not None and not self.sent:
            self.sent = True
            for message in self.messages:
                event.sender.send(message)

    def on_message(self, event):
        index = len(self.received)
        try:
            expected = self.expected[index]
            assert event.message.body == expected["body"], event.message.body
            for key, value in expected.get("properties", {}).items():
                assert event.message.properties[key] == value, event.message.properties
            self.received.append(event.message)
            delivery_index = len(self.received) - 1
            if self.outcome == "rejected":
                event.delivery.update(Delivery.REJECTED)
            elif self.outcome == "release-once" and delivery_index == 0:
                event.delivery.update(Delivery.RELEASED)
            elif self.outcome == "modified-failed" and delivery_index == 0:
                event.delivery.update(ModifiedDisposition(failed=True, undeliverable=False))
            elif self.outcome == "modified-undeliverable":
                event.delivery.update(ModifiedDisposition(failed=True, undeliverable=True))
            else:
                event.delivery.update(Delivery.ACCEPTED)
            event.delivery.settle()
            if len(self.received) == len(self.expected):
                self.finished = True
                event.reactor.schedule(0.1, self)
        except Exception as error:
            self.failure = error
            self.connection.close()

    def on_timer_task(self, event):
        if not self.finished:
            self.failure = TimeoutError(f"timed out receiving from {self.address}")
        self.connection.close()

    def on_disconnected(self, event):
        event.container.stop()


def run_roundtrip(name, address, messages, expected, **kwargs):
    test = RoundTrip(address, messages, expected, **kwargs)
    Container(test).run()
    if test.failure:
        raise test.failure
    assert len(test.received) == len(expected), (name, len(test.received))
    print(f"PASS {name}", flush=True)


class TransactionProbe(MessagingHandler):
    def __init__(self):
        super().__init__(auto_accept=False)
        self.transaction = None
        self.sender = None
        self.delivery = None
        self.phase = "abort"
        self.committed = False
        self.received = False
        self.failure = None

    def on_start(self, event):
        self.connection = event.container.connect(URL, reconnect=False)
        self.sender = event.container.create_sender(
            self.connection, "queue://proton-transactions"
        )
        event.container.create_receiver(
            self.connection, "queue://proton-transactions"
        )
        event.container.declare_transaction(self.connection, handler=self)
        event.reactor.schedule(TIMEOUT, self)

    def send_when_ready(self):
        if self.transaction and self.sender.credit and self.delivery is None:
            self.delivery = self.transaction.send(
                self.sender, Message(body=self.phase), tag=self.phase
            )

    def on_sendable(self, event):
        self.send_when_ready()

    def on_transaction_declared(self, event):
        self.transaction = event.transaction
        self.send_when_ready()

    def on_settled(self, event):
        if event.delivery == self.delivery:
            if self.phase == "abort":
                self.transaction.abort()
            else:
                self.transaction.commit()

    def on_transaction_aborted(self, event):
        self.phase = "commit"
        self.transaction = None
        self.delivery = None
        event.container.declare_transaction(self.connection, handler=self)

    def finish(self, event):
        if self.committed and self.received:
            self.connection.close()
            event.container.stop()

    def on_transaction_committed(self, event):
        self.committed = True
        self.finish(event)

    def on_message(self, event):
        if event.message.body != "commit" or self.received:
            self.failure = AssertionError(
                f"unexpected transactional delivery: {event.message.body!r}"
            )
            self.connection.close()
            event.container.stop()
            return
        event.delivery.update(Delivery.ACCEPTED)
        event.delivery.settle()
        self.received = True
        self.finish(event)

    def on_transport_error(self, event):
        self.failure = AssertionError(str(event.transport.condition))
        self.connection.close()
        event.container.stop()

    def on_timer_task(self, event):
        self.failure = TimeoutError("transactional commit/rollback timed out")
        self.connection.close()
        event.container.stop()


def run_transactions():
    test = TransactionProbe()
    Container(test).run()
    if test.failure:
        raise test.failure
    assert test.committed and test.received
    print("PASS transactional send commit and rollback", flush=True)


def run_durable_reconnect():
    address = "topic://proton-durable"
    client_id = "proton-durable-reconnect"

    def connect():
        container = Container()
        container.container_id = client_id
        return BlockingConnection(
            URL, container=container, timeout=TIMEOUT, reconnect=False
        )

    connection = connect()
    try:
        connection.create_receiver(
            address, name="saved", options=DurableSubscription()
        )
    finally:
        connection.close()

    producer = BlockingConnection(URL, timeout=TIMEOUT, reconnect=False)
    try:
        sender = producer.create_sender(address)
        sender.send(Message(body="published-while-offline", durable=True))
    finally:
        producer.close()

    connection = connect()
    try:
        receiver = connection.create_receiver(
            address, name="saved", options=DurableSubscription()
        )
        message = receiver.receive(timeout=TIMEOUT)
        assert message.body == "published-while-offline", message.body
        receiver.accept()
        receiver.close()
    finally:
        connection.close()
    print("PASS durable subscription reconnect and offline delivery", flush=True)


def main():
    run_transactions()
    run_durable_reconnect()
    run_roundtrip(
        "queue send/receive",
        "queue://proton-queue",
        [Message(body="queue-roundtrip")],
        [{"body": "queue-roundtrip"}],
    )
    run_roundtrip(
        "topic pub/sub",
        "topic://proton-topic",
        [Message(body="topic-roundtrip")],
        [{"body": "topic-roundtrip"}],
    )
    run_roundtrip(
        "jms-selector",
        "queue://proton-selector",
        [
            Message(body="red", properties={"color": "red"}),
            Message(body="blue", properties={"color": "blue"}),
        ],
        [{"body": "blue"}],
        selector="color = 'blue'",
    )
    run_roundtrip(
        "released delivery redelivery",
        "queue://proton-release",
        [Message(body="release-once")],
        [{"body": "release-once"}, {"body": "release-once"}],
        outcome="release-once",
    )
    run_roundtrip(
        "modified delivery-failed redelivery",
        "queue://proton-modified",
        [Message(body="modified-once")],
        [{"body": "modified-once"}, {"body": "modified-once"}],
        outcome="modified-failed",
    )
    run_reject_to_dlq("queue://proton-rejected", "reject-me")
    run_roundtrip(
        "modified undeliverable disposition",
        "queue://proton-undeliverable",
        [Message(body="undeliverable", durable=True)],
        [{"body": "undeliverable"}],
        outcome="modified-undeliverable",
    )
    large = "large-amqp-message/" * 12_000
    run_roundtrip(
        "multi-frame large message",
        "queue://proton-large",
        [Message(body=large)],
        [{"body": large}],
    )
    props = {"text": "typed", "integer": 42, "boolean": True, "decimal": 1.5}
    run_roundtrip(
        "typed application-properties",
        "queue://proton-properties",
        [Message(body="properties", properties=props)],
        [{"body": "properties", "properties": props}],
    )
    run_roundtrip(
        "delayed delivery",
        "queue://proton-delayed",
        [Message(body="delayed", annotations={symbol("x-opt-delivery-delay"): 500})],
        [{"body": "delayed"}],
    )
    run_roundtrip(
        "dynamic temporary queue",
        "queue://dynamic-placeholder",
        [Message(body="dynamic-request")],
        [{"body": "dynamic-request"}],
        dynamic=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise
