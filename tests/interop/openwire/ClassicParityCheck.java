import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.FixedCountSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.SessionId;

/** Run identical behavioral assertions against stock Classic or a Rust listener. */
public final class ClassicParityCheck {
    private static void require(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static List<String> drain(MessageConsumer consumer) throws Exception {
        var bodies = new ArrayList<String>();
        for (var message = consumer.receive(400); message != null; message = consumer.receive(100)) {
            require(message instanceof TextMessage, "expected TextMessage: " + message);
            bodies.add(((TextMessage) message).getText());
        }
        return bodies;
    }

    private static void expect(MessageConsumer consumer, List<String> expected) throws Exception {
        var actual = drain(consumer);
        require(expected.equals(actual), "expected " + expected + ", got " + actual);
    }

    private static void publish(Session session, String topic, String body) throws Exception {
        try (var producer = session.createProducer(session.createTopic(topic))) {
            producer.send(session.createTextMessage(body));
        }
    }

    private static List<String> history(String policy) {
        return switch (policy) {
            case "none" -> List.of();
            case "last_image" -> List.of("4");
            case "fixed_count" -> List.of("3", "4");
            default -> throw new IllegalArgumentException("unknown policy " + policy);
        };
    }

    private static void expectAdvisory(MessageConsumer consumer, Class<?> type) throws Exception {
        var message = consumer.receive(2000);
        require(message instanceof ActiveMQMessage, "missing advisory " + type.getSimpleName());
        var payload = ((ActiveMQMessage) message).getDataStructure();
        require(type.isInstance(payload), "expected " + type.getSimpleName() + ", got " + payload);
    }

    private static void check(String uri, String policy, int version, boolean alwaysRetroactive) throws Exception {
        var factory = new ActiveMQConnectionFactory(uri + (uri.contains("?") ? "&" : "?")
                + "wireFormat.version=" + version);
        factory.setAlwaysSyncSend(true);
        factory.setWatchTopicAdvisories(false);
        String prefix = "parity." + UUID.randomUUID();
        try (Connection connection = factory.createConnection();
             Connection other = factory.createConnection()) {
            connection.setClientID(prefix);
            connection.start();
            other.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session remote = other.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var identityQueue = session.createQueue(prefix + ".identity");
            try (var first = remote.createProducer(identityQueue);
                 var second = remote.createProducer(identityQueue);
                 var consumer = session.createConsumer(identityQueue)) {
                var one = remote.createTextMessage("one");
                var two = remote.createTextMessage("two");
                first.send(one);
                second.send(two);
                require(!one.getJMSMessageID().equals(two.getJMSMessageID()), "distinct producer IDs");
                var receivedOne = consumer.receive(2000);
                var receivedTwo = consumer.receive(2000);
                require(receivedOne != null && receivedTwo != null, "missing producer messages");
                require(one.getJMSMessageID().equals(receivedOne.getJMSMessageID()), "first message ID changed");
                require(two.getJMSMessageID().equals(receivedTwo.getJMSMessageID()), "second message ID changed");
            }
            try (var consumer = session.createConsumer(identityQueue)) {
                expect(consumer, List.of());
            }
            String topic = prefix + ".history";
            for (int i = 0; i < 5; i++) {
                publish(remote, topic, Integer.toString(i));
            }
            try (var ordinary = session.createConsumer(session.createTopic(topic))) {
                expect(ordinary, alwaysRetroactive ? history(policy) : List.of());
                publish(remote, topic, "live");
                expect(ordinary, List.of("live"));
            }
            // Refill the ring so the ordinary live publication does not change expectations.
            for (int i = 0; i < 5; i++) {
                publish(remote, topic, Integer.toString(i));
            }
            var retro = session.createTopic(topic + "?consumer.retroactive=true");
            try (var consumer = session.createConsumer(retro)) {
                expect(consumer, history(policy));
                publish(remote, topic, "next");
                expect(consumer, List.of("next"));
            }
            try (var consumer = session.createConsumer(retro, "color = 'blue'")) {
                expect(consumer, List.of());
            }
            String selectedTopic = prefix + ".selected";
            try (var producer = remote.createProducer(remote.createTopic(selectedTopic))) {
                var red = remote.createTextMessage("red");
                red.setStringProperty("color", "red");
                producer.send(red);
                var blue = remote.createTextMessage("blue");
                blue.setStringProperty("color", "blue");
                producer.send(blue);
            }
            try (var consumer = session.createConsumer(
                    session.createTopic(selectedTopic + "?consumer.retroactive=true"), "color = 'blue'")) {
                expect(consumer, "none".equals(policy) ? List.of() : List.of("blue"));
            }
            String wildcard = prefix + ".wildcard";
            publish(remote, wildcard + ".a", "a");
            publish(remote, wildcard + ".b", "b");
            try (var consumer = session.createConsumer(
                    session.createTopic(wildcard + ".>?consumer.retroactive=true"))) {
                var actual = drain(consumer);
                actual.sort(String::compareTo);
                require(actual.equals("none".equals(policy) ? List.of() : List.of("a", "b")),
                        "wildcard recovery: " + actual);
            }
            String localTopic = prefix + ".local";
            publish(session, localTopic, "local");
            try (var consumer = session.createConsumer(
                    session.createTopic(localTopic + "?consumer.retroactive=true"), null, true)) {
                expect(consumer, List.of());
            }
            String expiryTopic = prefix + ".expiry";
            try (var producer = remote.createProducer(remote.createTopic(expiryTopic))) {
                producer.setTimeToLive(50);
                producer.send(remote.createTextMessage("expired"));
            }
            Thread.sleep(100);
            try (var consumer = session.createConsumer(
                    session.createTopic(expiryTopic + "?consumer.retroactive=true"))) {
                expect(consumer, List.of());
            }
            String transactionalTopic = prefix + ".transaction";
            try (Session transaction = other.createSession(true, Session.SESSION_TRANSACTED)) {
                publish(transaction, transactionalTopic, "rolled-back");
                transaction.rollback();
                publish(transaction, transactionalTopic, "committed");
                transaction.commit();
            }
            try (var consumer = session.createConsumer(
                    session.createTopic(transactionalTopic + "?consumer.retroactive=true"))) {
                expect(consumer, "none".equals(policy) ? List.of() : List.of("committed"));
            }
            String durableTopic = prefix + ".durable";
            publish(remote, durableTopic, "before");
            var durableDestination = session.createTopic(durableTopic + "?consumer.retroactive=true");
            try (var consumer = session.createDurableSubscriber(durableDestination, "durable")) {
                expect(consumer, "none".equals(policy) ? List.of() : List.of("before"));
            }
            publish(remote, durableTopic, "offline");
            try (var consumer = session.createDurableSubscriber(durableDestination, "durable")) {
                expect(consumer, List.of("offline"));
            }
            session.unsubscribe("durable");

            // RemoveInfo(SessionId) must release its remote children even when
            // the client has not sent individual consumer/producer removals.
            var orphan = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String lifecycleQueue = prefix + ".session";
            try (var consumerAdvisories = session.createConsumer(
                    session.createTopic("ActiveMQ.Advisory.Consumer.Queue." + lifecycleQueue));
                 var producerAdvisories = session.createConsumer(
                    session.createTopic("ActiveMQ.Advisory.Producer.Queue." + lifecycleQueue))) {
                var child = (ActiveMQMessageConsumer) orphan.createConsumer(orphan.createQueue(lifecycleQueue));
                orphan.createProducer(orphan.createQueue(lifecycleQueue));
                expectAdvisory(consumerAdvisories, ConsumerInfo.class);
                expectAdvisory(producerAdvisories, ProducerInfo.class);
                ((ActiveMQConnection) connection).syncSendPacket(
                        new RemoveInfo(new SessionId(child.getConsumerId())));
                expectAdvisory(consumerAdvisories, RemoveInfo.class);
                expectAdvisory(producerAdvisories, RemoveInfo.class);
            }
            try (var survivor = session.createConsumer(session.createQueue(lifecycleQueue));
                 var producer = remote.createProducer(remote.createQueue(lifecycleQueue))) {
                producer.send(remote.createTextMessage("survives-session-remove"));
                expect(survivor, List.of("survives-session-remove"));
            }
            orphan.close();
            var removable = session.createQueue(prefix + ".removed");
            try (var producer = remote.createProducer(removable)) {
                producer.send(remote.createTextMessage("discard on removal"));
            }
            ((ActiveMQConnection) connection).destroyDestination(
                    (org.apache.activemq.command.ActiveMQDestination) removable);
            try (var consumer = session.createConsumer(removable);
                 var producer = remote.createProducer(removable)) {
                expect(consumer, List.of());
                boolean rejected = false;
                try {
                    ((ActiveMQConnection) connection).destroyDestination(
                            (org.apache.activemq.command.ActiveMQDestination) removable);
                } catch (jakarta.jms.JMSException expected) {
                    rejected = true;
                }
                require(rejected, "removal with active consumer should fail");
                producer.send(remote.createTextMessage("recreated"));
                expect(consumer, List.of("recreated"));
            }
            ((ActiveMQConnection) connection).destroyDestination(
                    (org.apache.activemq.command.ActiveMQDestination) removable);
            session.close();
            remote.close();
        }
        System.out.println("PASS " + policy + " v" + version + " alwaysRetroactive=" + alwaysRetroactive
                + ": recovery/selector/wildcard/noLocal/expiry/transaction/durable/session/advisory/destination lifecycle");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            throw new IllegalArgumentException(
                    "usage: ClassicParityCheck reference|BROKER_URI none|last_image|fixed_count [VERSION [ALWAYS_RETROACTIVE]]");
        }
        int version = args.length >= 3 ? Integer.parseInt(args[2]) : 12;
        if (version < 1 || version > 12) {
            throw new IllegalArgumentException("version must be 1 through 12");
        }
        boolean alwaysRetroactive = args.length == 4 && Boolean.parseBoolean(args[3]);
        if (!"reference".equals(args[0])) {
            check(args[0], args[1], version, alwaysRetroactive);
            return;
        }
        var broker = new BrokerService();
        broker.setBrokerName("classic-parity-reference");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        var entry = new PolicyEntry();
        entry.setAlwaysRetroactive(alwaysRetroactive);
        if ("last_image".equals(args[1])) {
            entry.setSubscriptionRecoveryPolicy(new LastImageSubscriptionRecoveryPolicy());
        } else if ("fixed_count".equals(args[1])) {
            var recovery = new FixedCountSubscriptionRecoveryPolicy();
            recovery.setMaximumSize(2);
            entry.setSubscriptionRecoveryPolicy(recovery);
        } else if (!"none".equals(args[1])) {
            throw new IllegalArgumentException("unknown policy " + args[1]);
        }
        var map = new PolicyMap();
        map.setDefaultEntry(entry);
        broker.setDestinationPolicy(map);
        var connector = broker.addConnector("tcp://127.0.0.1:0");
        try {
            broker.start();
            broker.waitUntilStarted();
            check(connector.getPublishableConnectString(), args[1], version, alwaysRetroactive);
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }
}
