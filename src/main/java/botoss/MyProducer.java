package botoss;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class MyProducer {
    public static void rate(String key, JSONObject jobj) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String s = "";
        try {
            logger.debug("curling yahooapis");
            s = getUrl("https://query.yahooapis.com/v1/public/yql?q=select+*+from+yahoo.finance.xchange+where+pair+" +
                    "=+%22USDRUB,EURRUB%22&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=");
            logger.debug("curled from yahooapis: " + s);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONArray arr = (new JSONObject(s)).getJSONObject("query").getJSONObject("results").getJSONArray("rate");
        String text = "";
        JSONArray params = jobj.getJSONArray("params");
        Double param = 1.;
        try{
            if (params.length() > 0)
                param = Double.parseDouble(params.get(0).toString());
        } catch(NumberFormatException ignore) {}
        for (int i = 0; i < arr.length(); i++) {
            text += arr.getJSONObject(i).getString("Name") + ": ";
            text += Double.toString((Math.round(Double.parseDouble(arr.getJSONObject(i).getString("Rate")) * param * 1000))/1000.) + "\n";
        }
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
        logger.debug("producer created");
        JSONObject ans = new JSONObject().put("connector-id", jobj.getString("connector-id")).put("text", text);
        producer.send(new ProducerRecord<>("to-connector", key, ans.toString()));
        logger.debug("producer send request created");

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

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
}
