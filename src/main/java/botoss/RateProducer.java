package botoss;

import botoss.source.CryptoSource;
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
    public static void main(String[] args) throws IOException {
        rate();
        System.out.println(rates);
    }
    private static final Logger logger = LoggerFactory.getLogger(RateProducer.class);
    private static volatile Map<String, Double> rates = new HashMap<>();
    private static Source source = new CryptoSource();
    private static Double factor = 1.;
    private static Boolean factorNotExist = true;

    static void rate(ConsumerRecord<String, String> record) throws IOException, JSONException {
        parseParams(new JSONObject(record.value()));
        sendMessage(record, getRateMessage());
    }

    static void btc(ConsumerRecord<String, String> record) throws IOException, JSONException {
        parseParams(new JSONObject(record.value()));
        sendMessage(record, getBtcMessage());
    }

    static void rate() throws IOException {
        source.takeInfo();
        rates = source.getRubInfo();
    }

    private static void parseParams(JSONObject jobj) throws IOException {
        factorNotExist = true;
        JSONArray params = jobj.getJSONArray("params");
        for (Object param : params) {
            switch (param.toString()) {
                case "-f":
                case "-force":
                    rate();
                    break;
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
    }

    private static String getRateMessage() {
        return "USD/RUB: " + ((rates.get("USD") == null) ? "██.███" : (Math.round(rates.get("USD") * factor * 1000)) / 1000.) +
                "\nEUR/RUB: " + ((rates.get("EUR") == null) ? "██.███" : (Math.round(rates.get("EUR") * factor * 1000)) / 1000.);
    }

    private static String getBtcMessage() {
        return "Тут могла бы быть ваша реклама!";
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