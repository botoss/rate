package botoss.source;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class YahooSource implements Source {
    private static final Logger logger = LoggerFactory.getLogger(YahooSource.class);
    private static final String RATE_URL = "https://query.yahooapis.com/v1/public/yql?q=select+*+from+yahoo.finance.xchange+where+pair+=+%22USDRUB,EURRUB%22&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=";
    private static String url = "";
    private static volatile JSONArray ratesArr;

    static {
        try {
            logger.debug("curling yahooapis");
            url = getUrl();
            logger.debug("curled from yahooapis: " + url);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    @Override
    public boolean takeInfo() {
        ratesArr = (new JSONObject(url)).getJSONObject("query").getJSONObject("results").getJSONArray("rate");
        return true;
    }

    @Override
    public Map<String, Double> getRubInfo() {
        Map<String, Double> rates = new HashMap<>();
        for (int i = 0; i < ratesArr.length(); i++) {
            rates.put(ratesArr.getJSONObject(i).getString("Name"), Double.parseDouble(ratesArr.getJSONObject(i).getString("Rate")));
        }
        return rates;
    }
}
