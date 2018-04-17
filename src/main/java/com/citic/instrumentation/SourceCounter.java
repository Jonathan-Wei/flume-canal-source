package com.citic.instrumentation;

import com.citic.helper.RegexHashMap;
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
    private Map<String, String> tableRegexMap;
    private Gson gson = new Gson();

    public SourceCounter(String name, String[] attributes) {
        super(Type.OTHER, name, attributes);
        this.attributes = attributes;

        localMap = Maps.newHashMap();
        tableRegexMap = new RegexHashMap<>();
        for (String attr : attributes) {
            tableRegexMap.put(attr, attr);
        }
    }

    public String[] getAttributes() {
        return this.attributes;
    }

    public long incrementTableReceivedCount(String tableName) {
        // 用户配置的可能为 正则表达式,但这里传递进来的是解析后的表名
        // eg: attribute = test\\.test.*, tableName = test.test1
        String regexTableName = this.tableRegexMap.get(tableName);
        return increment(regexTableName);
    }

    @Override
    public String getReceivedTableCount() {
        for (String attribute : attributes) {
            localMap.put(attribute, get(attribute));
        }
        return gson.toJson(localMap);
    }
}
