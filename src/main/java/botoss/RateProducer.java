package botoss;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;


public class RateProducer {
    private static final String RATE_URL = "https://query.yahooapis.com/v1/public/yql?q=select+*+from+yahoo.finance.xchange+where+pair+=+%22USDRUB,EURRUB%22&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=";
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static volatile JSONArray ratesArr;
    private static String url = "";

    static {
        try {
            logger.debug("curling yahooapis");
            url = getUrl();
            //logger.debug("curled from yahooapis: " + url);
            logger.debug("curled from yahooapis:  + url");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void rate() {
        ratesArr = (new JSONObject(url)).getJSONObject("query").getJSONObject("results").getJSONArray("rate");
    }

    static void rate(ConsumerRecord<String, String> record) throws IOException {
        String message = getMessage(new JSONObject(record.value()));
        sendMessage(record, message);
    }

    private static String getMessage(JSONObject jobj) {
        JSONArray params = jobj.getJSONArray("params");
        Double factor = 1.;
        boolean factorNotExist = true;
        for (Object param : params) {
            switch (param.toString()) {
                case "-f":
                case "-force":
                    rate();
                    continue;
                default:
                    if (factorNotExist) {
                        try {
                            factor = Double.parseDouble(params.get(0).toString());
                            factorNotExist = false;
                        } catch (NumberFormatException ignore) {
                        }
                    }
                    continue;
            }
        }
        return (ratesArr == null) ? "loading..." : getText(ratesArr, factor);
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

    private static String getText(JSONArray arr, Double param) {
        String text = "";
        for (int i = 0; i < arr.length(); i++) {
            text += arr.getJSONObject(i).getString("Name") + ": ";
            text += Double.toString((Math.round(Double.parseDouble(arr.getJSONObject(i).getString("Rate")) * param * 1000)) / 1000.) + "\n";
        }
        return text;
    }

    private static String getUrl() throws IOException {
        HttpGet req = new HttpGet(RATE_URL);
        req.setHeader("Content-Type", "application/json");
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(req)) {
            InputStream inputStream = response.getEntity().getContent();
            return IOUtils.toString(inputStream);
        }
    }
}