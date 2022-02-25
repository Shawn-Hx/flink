package org.apache.flink.runtime.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tools {
    public static final Logger log =
            LoggerFactory.getLogger(Tools.class);

    public static void printThreadTrace(){
        StringBuilder sb = new StringBuilder("[JY] THREAD TRACE===========================\n");
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            sb.append(stackTraceElement);
            sb.append('\n');
        }
        log.info(sb.toString());
    }
}
