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
            logger.debug("curled from yahooapis: " + url);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void rate() {
        ratesArr = (new JSONObject(url)).getJSONObject("query").getJSONObject("results").getJSONArray("rate");
    }

    static void rate(ConsumerRecord<String, String> record) throws IOException {
        JSONObject jobj = new JSONObject(record.value());
        Properties props = new Properties();
        try (Reader propsReader = new FileReader("/kafka.properties")) {
            props.load(propsReader);
        }
        JSONArray params = jobj.getJSONArray("params");
        Double param = 1.;
        try {
            if (params.length() > 0)
                param = Double.parseDouble(params.get(0).toString());
        } catch (NumberFormatException ignore) {
        }
        String message = getMessage(ratesArr, param);

       /* sendMessage(record.key(), jobj.getString("connector-id"), props, message);
    }

    private static void sendMessage(String key, String connectorId, Properties props, String message) {*/
       /* String key = record.key();
        String connectorId = jobj.getString("connector-id");*/

        Producer<String, String> producer = new KafkaProducer<>(props);
        logger.debug("producer created");
        JSONObject ans = new JSONObject();
        try {
            ans.put("connector-id", jobj.getString("connector-id"));
        } catch (JSONException ignore) {
            // it's ok for now not to have connector-id in message
        }
        ans.put("text", message);
        producer.send(new ProducerRecord<>("to-connector", record.key(), ans.toString()));
        logger.debug("producer send request created");

        producer.close();
    }

    private static String getMessage(JSONArray arr, Double param) {
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
