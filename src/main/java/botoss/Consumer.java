package botoss;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (Reader propsReader = new FileReader("/kafka.properties")) {
            props.load(propsReader);
        }
        props.put("group.id", "rate-module");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("to-module"));
        logger.info("Subscribed to topic");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record from topic: key = " + record.key() + "; value = " + record.value());
                String command = (new JSONObject(record.value())).getString("command");
                if (rateCommand(command)) {
                    MyProducer.rate(record.key(), new JSONObject(record.value()));
                }
            }
        }
    }

    private static boolean rateCommand(String command) {
        return "курс".equals(command) ||
                "rate".equals(command) ||
                "kurs".equals(command);
    }

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
}
