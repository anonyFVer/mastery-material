package org.apache.dubbo.monitor;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import java.util.List;

public interface MonitorService {

    String APPLICATION = "application";

    String INTERFACE = "interface";

    String METHOD = "method";

    String GROUP = "group";

    String VERSION = "version";

    String CONSUMER = "consumer";

    String PROVIDER = "provider";

    String TIMESTAMP = "timestamp";

    String SUCCESS = "success";

    String FAILURE = "failure";

    String INPUT = Constants.INPUT_KEY;

    String OUTPUT = Constants.OUTPUT_KEY;

    String ELAPSED = "elapsed";

    String CONCURRENT = "concurrent";

    String MAX_INPUT = "max.input";

    String MAX_OUTPUT = "max.output";

    String MAX_ELAPSED = "max.elapsed";

    String MAX_CONCURRENT = "max.concurrent";

    void collect(URL statistics);

    List<URL> lookup(URL query);
}