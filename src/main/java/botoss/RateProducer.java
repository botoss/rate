package botoss;

import botoss.source.ExchangeSource;
import botoss.source.Source;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class RateProducer {
    private static final Logger logger = LoggerFactory.getLogger(RateProducer.class);
    private static volatile Map<String, Double> rates = new HashMap<>();
    private static Source source = new ExchangeSource();

    static void rate(ConsumerRecord<String, String> record) throws IOException, JSONException {
        String message = getMessage(new JSONObject(record.value()));
        sendMessage(record, message);
    }

    static void rate() {
        source.takeInfo();
        rates = source.getRubInfo();
    }

    private static String getMessage(JSONObject jobj) {
        JSONArray params = jobj.getJSONArray("params");
        Double factor = 1.;
        boolean factorNotExist = true;
        for (Object param : params) {
            switch (param.toString()) {
                /*case "-f":
                case "-force":
                    rate();
                    break;*/
                default:
                    if (factorNotExist) {
                        try {
                            factor = Double.parseDouble(param.toString());
                            factorNotExist = false;
                        } catch (NumberFormatException ignore) {
                        }
                    }
                    break;
            }
        }
        return getText(factor);
    }

    private static String getText(Double factor) {
        return "USD/RUB: " + ((rates.get("USD") == null) ? "██.███" : (Math.round(rates.get("USD") * factor * 1000)) / 1000.) +
                "\nEUR/RUB: " + ((rates.get("EUR") == null) ? "██.███" : (Math.round(rates.get("EUR") * factor * 1000)) / 1000.);
    }

    private static void sendMessage(ConsumerRecord<String, String> record, String message) throws IOException {
        Properties props = new Properties();
        try (Reader propsReader = new FileReader("/kafka.properties")) {
            props.load(propsReader);
        }
        Producer<String, String> producer = new KafkaProducer<>(props);
        logger.debug("producer created");
        JSONObject ans = new JSONObject();
        try {
            ans.put("connector-id", new JSONObject(record.value()).getString("connector-id"));
        } catch (JSONException ignore) {
            // it's ok for now not to have connector-id in message
        }

        ans.put("text", message);
        producer.send(new ProducerRecord<>("to-connector", record.key(), ans.toString()));
        logger.debug("producer send request created");

        producer.close();
    }
}