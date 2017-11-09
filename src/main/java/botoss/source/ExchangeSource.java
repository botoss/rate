package botoss.source;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ExchangeSource implements Source {
    private static final Logger logger = LoggerFactory.getLogger(ExchangeSource.class);
    private static final String RATE_URL = "https://openexchangerates.org/api/latest.json?app_id=74ea1d360294459283dd6827b2047679";
    private static String url = "";
    private static volatile JSONObject ratesArr;

    /*          "base": "USD",
                "ratesArr": {
                        ...
                    "EUR": 0.858539,
                        ...
                    "RUB": 59.2957,
                        ...
                    "USD": 1,
                        ...
                }
    */
    static {
        try {
            logger.debug("curling openexchangerates");
            url = getUrl();
            logger.debug("curled from openexchangerates: " + url);
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
        ratesArr = new JSONObject(url).getJSONObject("rates");
        return true;
    }

    @Override
    public Map<String, Double> getRubInfo() {
        Map<String, Object> ratesObj = ratesArr.toMap();
        Map<String, Double> rates = new HashMap<>();
        Double rubRate = Double.parseDouble(ratesObj.get("RUB").toString());
        for (Map.Entry<String, Object> rateObj : ratesObj.entrySet()) {
            rates.put(rateObj.getKey(), rubRate / Double.parseDouble(rateObj.getValue().toString()));
        }
        return rates;
    }
}
