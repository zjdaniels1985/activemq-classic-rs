import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import jakarta.jms.Connection;
import jakarta.jms.MapMessage;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

/** Run with activemq-client 6.2.6 on the classpath; arguments: OpenWire URI, STOMP port. */
public class MapJsonCheck {
    private static String frame(InputStream input) throws Exception {
        StringBuilder result = new StringBuilder();
        int next;
        while ((next = input.read()) > 0) {
            result.append((char) next);
        }
        if (next < 0) {
            throw new AssertionError("STOMP connection closed before response");
        }
        return result.toString();
    }

    public static void main(String[] args) throws Exception {
        String queue = "stomp.map." + UUID.randomUUID();
        var factory = new ActiveMQConnectionFactory(args[0]);
        try (Connection connection = factory.createConnection();
             Socket stomp = new Socket("127.0.0.1", Integer.parseInt(args[1]))) {
            stomp.setSoTimeout(5000);
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
            var output = stomp.getOutputStream();
            output.write("STOMP\naccept-version:1.2\nhost:localhost\n\n\0"
                .getBytes(StandardCharsets.UTF_8));
            if (!frame(stomp.getInputStream()).startsWith("CONNECTED\n")) {
                throw new AssertionError("STOMP handshake failed");
            }
            String json = """
                {"map":{"entry":[
                    {"string":["name","order"]},
                    {"string":"count","int":42},
                    {"string":"enabled","boolean":true},
                    {"string":"binary","byte-array":"AP9B"}
                ]}}
                """;
            output.write(("SEND\ndestination:/queue/" + queue
                + "\ntransformation:jms-map-json\nreceipt:sent\n\n" + json + "\0")
                .getBytes(StandardCharsets.UTF_8));
            if (!frame(stomp.getInputStream()).startsWith("RECEIPT\n")) {
                throw new AssertionError("STOMP map send failed");
            }
            var received = consumer.receive(5000);
            if (!(received instanceof MapMessage map)) {
                throw new AssertionError("expected JMS MapMessage, got " + received);
            }
            if (!"order".equals(map.getString("name"))
                || !(map.getObject("count") instanceof Integer)
                || map.getInt("count") != 42 || !map.getBoolean("enabled")
                || !java.util.Arrays.equals(map.getBytes("binary"), new byte[]{0, -1, 65})) {
                throw new AssertionError("JMS map types or values were not preserved");
            }
            consumer.close();
            session.close();
            System.out.println("PASS STOMP Classic map JSON to Java JMS MapMessage");
        }
    }
}
