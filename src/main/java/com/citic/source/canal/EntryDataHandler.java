package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.Utility;
import com.citic.instrumentation.SourceCounter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.ListUtils;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static com.citic.source.canal.CanalSourceConstants.*;


class EntryDataHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(EntryDataHandler.class);
    private static final Gson GSON = new Gson();
    private static final Type JSONType = new TypeToken<Map<String, Object>>(){}.getType();
    private static final String MIX_TOPIC_PREFIX = "mix_";
    private static final String MIX_TOPIC_DATA_FIELD_NAME = "data";

    /*
    * 获取表的主键,用于kafka的分区key
    * */
    private static String getPK(CanalEntry.RowData rowData) {
        StringBuilder pk = null;
        for(CanalEntry.Column column : rowData.getAfterColumnsList()) {
            if (column.getIsKey()) {
                if (pk == null)
                    pk = new StringBuilder();
                pk.append(column.getValue());
            }
        }
        if (pk == null)
            return null;
        else
            return pk.toString();
    }

    /*
    * db.table 作为key
    * */
    private static String getTableKeyName(CanalEntry.Header entryHeader) {
        String table = entryHeader.getTableName();
        String database = entryHeader.getSchemaName();
        return database + '.' + table;
    }

    /*
    * 获取数据 Event
    * */
    static Event getDataEvent(CanalEntry.RowData rowData,
                              CanalEntry.Header entryHeader,
                              CanalEntry.EventType eventType,
                              CanalConf canalConf,
                              SourceCounter tableCounter) {
        String keyName = getTableKeyName(entryHeader);
        String topic = canalConf.getTableTopic(keyName);
        // 处理行数据
        Map<String, String> eventData = handleRowData(rowData, entryHeader, eventType, topic, canalConf);
        LOGGER.debug("eventData handleRowData:{}", eventData);
        try {
            // 监控表数据
            tableCounter.incrementTableReceivedCount(keyName);
        } catch (Exception e) {
            LOGGER.error("table name:{}, attributes:{}", keyName, tableCounter.getAttributes());
            LOGGER.error(e.getMessage());
        }

        String pk = getPK(rowData);
        // 处理 event Header
        LOGGER.debug("RowData pk:{}", pk);
        Map<String, String> header = handleRowDataHeader(topic, pk);

        return dataToAvroEvent(eventData, header, topic, canalConf);
    }

    /*
    * 处理 Event Header 获取数据的 topic
    * */
    private static Map<String, String> handleRowDataHeader(String topic, String kafkaKey) {
        Map<String, String> header = Maps.newHashMap();
        if (kafkaKey != null){
            // 将表的主键作为kafka分区的key
            header.put(HEADER_KEY, kafkaKey);
        }
        header.put(HEADER_TOPIC, topic);
        return header;
    }

    private static boolean isDataToMixTopic(String topic) {
        Preconditions.checkArgument(Strings.isNullOrEmpty(topic), "topic cannot empty");
        return topic.startsWith(MIX_TOPIC_PREFIX);
    }

    /*
    * 将 data, header 转换为 Avro Event 格式
    * */
    private static Event dataToAvroEvent(Map<String, String> eventData,
                                         Map<String, String> eventHeader,
                                         String topic,
                                         CanalConf canalConf) {

        List<String> schemaFieldList  = canalConf.getTopicToSchemaFields().get(topic);

        List<String> attrList = Lists.newArrayList(META_FIELD_TABLE, META_FIELD_TS, META_FIELD_DB,
                                                   META_FIELD_TYPE, META_FIELD_AGENT, META_FIELD_FROM);

        String schemaName = canalConf.getTopicToSchemaMap().get(topic);

        Schema.Parser parser = new Schema.Parser();
        String schemaString = Utility.getTableFieldSchema(ListUtils.union(schemaFieldList, attrList), schemaName);
        Schema schema = parser.parse(schemaString);

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        GenericRecord avroRecord = new GenericData.Record(schema);

        for (String fieldStr: schemaFieldList) {
            String tableField = canalConf.getTopicSchemaFieldToTableField().get(topic, fieldStr);
            avroRecord.put(fieldStr, eventData.getOrDefault(tableField, ""));
        }

        for (String fieldStr: attrList) {
            avroRecord.put(fieldStr, eventData.getOrDefault(fieldStr, ""));
        }

        byte[] eventBody = recordInjection.apply(avroRecord);

        // 用于sink解析
        eventHeader.put(HEADER_SCHEMA, schemaString);
        return EventBuilder.withBody(eventBody,eventHeader);
    }

    /*
    * 处理行数据，并添加其他字段信息
    * */
    private static Map<String, String> handleRowData(CanalEntry.RowData rowData,
                                                     CanalEntry.Header entryHeader,
                                                     CanalEntry.EventType eventType,
                                                     String topic,
                                                     CanalConf canalConf) {
        Map<String, String> eventMap = Maps.newHashMap();
        Map<String, String> rowDataMap;

        if (eventType == CanalEntry.EventType.DELETE) {
            // 删除事件 getAfterColumnsList 数据为空
            rowDataMap = convertColumnListToMap(rowData.getBeforeColumnsList(), entryHeader);
        } else {
            rowDataMap = convertColumnListToMap(rowData.getAfterColumnsList(), entryHeader);
        }

        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, String.valueOf(Math.round(entryHeader.getExecuteTime() / 1000)));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_TYPE, eventType.toString());
        eventMap.put(META_FIELD_AGENT, canalConf.getIPAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDBIP());

        /*
        * topic 以 mix_开头表示所有行数据放入__data 字符串中,不做 arvo schema 校验
        * 数据统一放入 data 中
        * */
        if(isDataToMixTopic(topic)) {
            String dataJSONString =  GSON.toJson(rowDataMap, JSONType);
            eventMap.put(MIX_TOPIC_DATA_FIELD_NAME, dataJSONString);
        } else {
            eventMap.putAll(rowDataMap);
        }

        return  eventMap;
    }

    /*
    * 对列数据进行解析
    * */
    private static Map<String, String> convertColumnListToMap(List<CanalEntry.Column> columns, CanalEntry.Header entryHeader) {
        Map<String, String> rowMap = Maps.newHashMap();

        String keyName = entryHeader.getSchemaName() + '.' + entryHeader.getTableName();
        for(CanalEntry.Column column : columns) {
            int sqlType = column.getSqlType();
            String stringValue = column.getValue();
            String colValue;

            try {
                switch (sqlType) {
                    /*
                    * date 2018-04-02
                    * time 02:34:51
                    * datetime 2018-04-02 11:43:16
                    * timestamp 2018-04-02 11:45:02
                    * mysql 默认格式如上，现在不做处理后续根据需要再更改
                    * */
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP: {
                        colValue = stringValue;
                        break;
                    }
                    default: {
                        colValue = stringValue;
                        break;
                    }
                }
            } catch (NumberFormatException numberFormatException) {
                colValue = null;
            } catch (Exception exception) {
                LOGGER.warn("convert row data exception", exception);
                colValue = null;
            }
            rowMap.put(column.getName(), colValue);
        }
        return rowMap;
    }

}
