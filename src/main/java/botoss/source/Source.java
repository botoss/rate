package botoss.source;

import java.util.Map;

public interface Source {
    boolean takeInfo();

    Map<String, Double> getRubInfo();
}
