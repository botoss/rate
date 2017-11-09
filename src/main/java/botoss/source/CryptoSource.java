package botoss.source;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class CryptoSource implements Source {
    private static final String RATE_URL = "https://api.cryptonator.com/api/ticker/"; // + "usd-rub"
    private static volatile Map<String, Double> ratesArr;

    @Override
    public boolean takeInfo() throws IOException {
        ratesArr = new HashMap<>();
        putVal("USD");
        putVal("EUR");
        putVal("BTC");
        putVal("BCH");
        putVal("BTG");
        return true;
    }

    private static void putVal(String val) throws IOException {
        String info;
        HttpGet req = new HttpGet(RATE_URL + val + "-rub");
        req.setHeader("Content-Type", "application/json");
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(req)) {
            InputStream inputStream = response.getEntity().getContent();
            info = IOUtils.toString(inputStream);
        }
        ratesArr.put(val, (Double) new JSONObject(info).getJSONObject("ticker").get("prise"));
    }

    @Override
    public Map<String, Double> getRubInfo() {
        return ratesArr;
    }
}
