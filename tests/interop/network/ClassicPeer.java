import java.nio.file.Files;
import java.nio.file.Path;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * Starts a stock Classic peer or verifies forwarding against it.
 * Arguments: serve TCP-port ready-file | check Rust-URI Classic-URI.
 */
public class ClassicPeer {
    private static void send(Session session, String destination, String body) throws Exception {
        try (MessageProducer producer = session.createProducer(session.createQueue(destination))) {
            producer.send(session.createTextMessage(body));
        }
    }

    private static void receive(MessageConsumer consumer, String expected) throws Exception {
        var message = consumer.receive(10000);
        if (!(message instanceof TextMessage text) || !expected.equals(text.getText())) {
            throw new AssertionError("expected " + expected + ", got " + message);
        }
        if (consumer.receive(400) != null) {
            throw new AssertionError("unexpected duplicate after " + expected);
        }
    }

    private static void temporaryReply(Session requester, Session responder, String queue)
            throws Exception {
        var replyQueue = requester.createTemporaryQueue();
        try (MessageConsumer replies = requester.createConsumer(replyQueue);
             MessageConsumer requests = responder.createConsumer(responder.createQueue(queue));
             MessageProducer requestProducer = requester.createProducer(requester.createQueue(queue));
             MessageProducer replyProducer = responder.createProducer(null)) {
            Thread.sleep(1000);
            var request = requester.createTextMessage("request");
            request.setJMSReplyTo(replyQueue);
            requestProducer.send(request);
            var received = requests.receive(10000);
            if (received == null || received.getJMSReplyTo() == null) {
                throw new AssertionError("request or temporary reply destination missing");
            }
            replyProducer.send(received.getJMSReplyTo(), responder.createTextMessage("reply"));
            receive(replies, "reply");
        }
        replyQueue.delete();
    }

    public static void main(String[] args) throws Exception {
        if ("serve".equals(args[0])) {
            var broker = new BrokerService();
            broker.setBrokerName("stock-classic-peer");
            broker.setPersistent(false);
            broker.setUseJmx(false);
            broker.setUseShutdownHook(false);
            broker.addConnector("tcp://127.0.0.1:" + Integer.parseInt(args[1]));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    broker.stop();
                } catch (Exception error) {
                    error.printStackTrace();
                }
            }));
            broker.start();
            broker.waitUntilStarted();
            Files.writeString(Path.of(args[2]), "ready");
            System.out.println("READY stock Classic 6.2.6 peer");
            new java.util.concurrent.CountDownLatch(1).await();
            return;
        }
        var rustFactory = new ActiveMQConnectionFactory(args[1]);
        var classicFactory = new ActiveMQConnectionFactory(args[2]);
        String prefix = "interop." + java.util.UUID.randomUUID();
        try (Connection rust = rustFactory.createConnection();
             Connection classic = classicFactory.createConnection()) {
            rust.start();
            classic.start();
            Session local = rust.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session remote = classic.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try (MessageConsumer remoteConsumer = remote.createConsumer(
                     remote.createQueue(prefix + ".outbound"));
                 MessageConsumer localConsumer = local.createConsumer(
                     local.createQueue(prefix + ".inbound"))) {
                Thread.sleep(1000);
                send(local, prefix + ".outbound", "rust-to-classic");
                receive(remoteConsumer, "rust-to-classic");
                send(remote, prefix + ".inbound", "classic-to-rust");
                receive(localConsumer, "classic-to-rust");
            }
            try (MessageConsumer remoteTopic = remote.createConsumer(
                     remote.createTopic(prefix + ".topic.outbound"));
                 MessageConsumer localTopic = local.createConsumer(
                     local.createTopic(prefix + ".topic.inbound"));
                 MessageProducer outbound = local.createProducer(
                     local.createTopic(prefix + ".topic.outbound"));
                 MessageProducer inbound = remote.createProducer(
                     remote.createTopic(prefix + ".topic.inbound"))) {
                Thread.sleep(1000);
                outbound.send(local.createTextMessage("topic-to-classic"));
                receive(remoteTopic, "topic-to-classic");
                inbound.send(remote.createTextMessage("topic-to-rust"));
                receive(localTopic, "topic-to-rust");
            }
            temporaryReply(local, remote, prefix + ".request.outbound");
            temporaryReply(remote, local, prefix + ".request.inbound");
            local.close();
            remote.close();
        }
        System.out.println("PASS duplex queues, topics and temporary replies with stock Classic 6.2.6");
    }
}
