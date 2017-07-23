package botoss;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class MyProducer {
    public static void rate(String key, String connectorName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.64:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String s = null;
        try {
            s = getUrl("https://query.yahooapis.com/v1/public/yql?q=select+*+from+yahoo.finance.xchange+where+pair+" +
                    "=+%22USDRUB,EURRUB%22&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=");
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONArray arr = (new JSONObject(s)).getJSONObject("query").getJSONObject("results").getJSONArray("rate");
        String text = "";
        for (int i = 0; i < arr.length(); i++) {
            text += arr.getJSONObject(i).getString("Name") + ": ";
            text += arr.getJSONObject(i).getString("Rate") + "\n";
        }
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
        JSONObject ans = new JSONObject().put("connector-id", connectorName).put("text", text);
        producer.send(new ProducerRecord<String, String>("to-connector",  key, ans.toString()));

        producer.close();
    }

    private static String getUrl(String uri) throws IOException {
        HttpGet req = new HttpGet(uri);
        req.setHeader("Content-Type", "application/json");
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(req)) {
            InputStream inputStream = response.getEntity().getContent();
            return IOUtils.toString(inputStream);
        }
    }
}
