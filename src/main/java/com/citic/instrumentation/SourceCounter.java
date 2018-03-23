package com.citic.instrumentation;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.flume.instrumentation.MonitoredCounterGroup;
import java.util.Map;

public class SourceCounter extends MonitoredCounterGroup implements
        SourceCounterMBean {

    private String[] attributes;
    private Map<String, Long> localMap;
    private Gson gson = new Gson();

    public SourceCounter(String name, String[] attributes) {
        super(Type.OTHER, name, attributes);
        this.attributes = attributes;

        localMap = Maps.newHashMap();
    }

    public long incrementTableReceivedCount(String tableName) {
        return increment(tableName);
    }

    @Override
    public String getReceivedTableCount() {
        for (String attribute : attributes) {
            localMap.put(attribute, get(attribute));
        }
        return gson.toJson(localMap);
    }
}
