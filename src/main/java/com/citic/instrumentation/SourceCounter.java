package com.citic.instrumentation;

import com.citic.source.canal.CanalConf;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.flume.instrumentation.MonitoredCounterGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SourceCounter extends MonitoredCounterGroup implements
        SourceCounterMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoredCounterGroup.class);

    private String[] attributes;
    private Map<String, Long> localMap;
    private Gson gson = new Gson();

    public SourceCounter(String name, String[] attributes) {
        super(Type.OTHER, name, attributes);
        this.attributes = attributes;

        localMap = Maps.newHashMap();
    }

    public long incrementTableReceivedCount(String tableName) {
        LOGGER.debug("tableName:{}", tableName);
        for (String attr: this.attributes) {
            LOGGER.debug("attr:{}", attr);
        }
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
