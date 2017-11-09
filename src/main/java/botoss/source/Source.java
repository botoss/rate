package botoss.source;

import java.io.IOException;
import java.util.Map;

public interface Source {
    boolean takeInfo() throws IOException;

    Map<String, Double> getRubInfo();
}
