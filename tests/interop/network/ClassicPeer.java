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
        try (Connection rust = rustFactory.createConnection();
             Connection classic = classicFactory.createConnection()) {
            rust.start();
            classic.start();
            Session local = rust.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session remote = classic.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try (MessageConsumer remoteConsumer = remote.createConsumer(
                     remote.createQueue("interop.outbound"));
                 MessageConsumer localConsumer = local.createConsumer(
                     local.createQueue("interop.inbound"))) {
                Thread.sleep(1000);
                send(local, "interop.outbound", "rust-to-classic");
                receive(remoteConsumer, "rust-to-classic");
                send(remote, "interop.inbound", "classic-to-rust");
                receive(localConsumer, "classic-to-rust");
            }
            local.close();
            remote.close();
        }
        System.out.println("PASS duplex queue forwarding with stock Classic 6.2.6");
    }
}
