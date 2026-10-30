import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;

public final class InteropCheck {
    private static final String BROKER = System.getenv().getOrDefault(
            "OPENWIRE_INTEROP_URL", "tcp://127.0.0.1:61616");

    public static void main(String[] args) throws Exception {
        int first = args.length == 0 ? 4 : Integer.parseInt(args[0]);
        int last = args.length < 2 ? (args.length == 0 ? 12 : first) : Integer.parseInt(args[1]);
        for (int version = first; version <= last; version++) {
            runVersion(version);
            System.out.println("OpenWire v" + version + ": queue, selector, topic, transaction OK");
        }
    }

    private static void runVersion(int version) throws Exception {
        String uri = BROKER + "?wireFormat.version=" + version
                + "&wireFormat.maxInactivityDuration=0";
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
        factory.setWatchTopicAdvisories(true);
        factory.setProducerWindowSize(256);
        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("openwire.interop.v" + version);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer selected = session.createConsumer(queue, "marker = 'match'");

            TextMessage rolledBack = session.createTextMessage("must not be visible");
            rolledBack.setStringProperty("marker", "match");
            System.out.println("v" + version + ": sending rollback message");
            producer.send(rolledBack);
            System.out.println("v" + version + ": rolling back");
            session.rollback();

            TextMessage queued = session.createTextMessage("queue-v" + version);
            queued.setStringProperty("marker", "match");
            System.out.println("v" + version + ": sending committed message");
            producer.send(queued);
            TextMessage skipped = session.createTextMessage("selector mismatch");
            skipped.setStringProperty("marker", "skip");
            producer.send(skipped);
            System.out.println("v" + version + ": committing");
            session.commit();

            connection.start();
            Message received = selected.receive(4000);
            require(received instanceof TextMessage, "selected queue message missing");
            require(("queue-v" + version).equals(((TextMessage) received).getText()),
                    "queue transaction or selector mismatch");
            session.commit();

            Topic topic = session.createTopic("openwire.interop.topic.v" + version);
            MessageConsumer subscriber = session.createConsumer(topic);
            MessageProducer publisher = session.createProducer(topic);
            publisher.send(session.createTextMessage("topic-v" + version));
            session.commit();
            Message topicMessage = subscriber.receive(4000);
            require(topicMessage instanceof TextMessage, "topic dispatch missing");
            require(("topic-v" + version).equals(((TextMessage) topicMessage).getText()),
                    "topic payload mismatch");
            session.commit();
            session.close();
        }
    }

    private static void require(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }
}
