package botoss;

import botoss.source.CryptoSource;
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
    private static Source source = new CryptoSource();
    private static Double factor = 1.;
    private static Boolean factorNotExist = true;
    private static final Double BTC = 0.04423945;

    static void rate(ConsumerRecord<String, String> record) throws IOException, JSONException {
        parseParams(new JSONObject(record.value()));
        sendMessage(record, getRateMessage());
    }

    static void btc(ConsumerRecord<String, String> record) throws IOException, JSONException {
        parseParams(new JSONObject(record.value()));
        sendMessage(record, getBtcMessage());
    }

    static void maxBtc(ConsumerRecord<String, String> record) throws IOException, JSONException {
        parseParams(new JSONObject(record.value()));
        sendMessage(record, getMaxBtcMessage());
    }

    static void rate() throws IOException {
        source.takeInfo();
        rates = source.getRubInfo();
    }

    private static void parseParams(JSONObject jobj) throws IOException {
        factorNotExist = true;
        factor = 1.;
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
        return getRubValAns("USD") + getRubValAns("EUR");
    }

    private static String getBtcMessage() {
        return getUsdValAns("BTC") + getUsdValAns("BCH") + getUsdValAns("BTG");
    }

    private static String getMaxBtcMessage() {
        return getMaxAnswer();
    }

    private static String getUsdValAns(String val) {
        return val + ": $" + ((rates.get(val) == null) ? "████.██" : (Math.round(rates.get(val) / rates.get("USD") * factor * 1000)) / 1000.) + "\n";
    }

    private static String getRubValAns(String val) {
        return val + "/RUB: " + ((rates.get(val) == null) ? "████.██" : (Math.round(rates.get(val) * factor * 1000)) / 1000.) + "\n";
    }

    private static String getMaxAnswer() {
        return "BTC: " + BTC +
                "\nUSD: " + Math.round(BTC * getSumBtc() / rates.get("USD") * 100) / 100. +
                "\nRUB: " + Math.round(BTC * getSumBtc() * 100) / 100.;
    }

    private static Double getSumBtc() {
        return rates.get("BTC") + rates.get("BCH") + rates.get("BTG");
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