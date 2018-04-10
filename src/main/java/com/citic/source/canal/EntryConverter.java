/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.Utility;
import com.citic.instrumentation.SourceCounter;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
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
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class EntryConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntryConverter.class);
    private static final Gson GSON = new Gson();
    private static Type TOKEN_TYPE = new TypeToken<Map<String, Object>>(){}.getType();

    private Long numberInTransaction = 0L;
    private CanalConf canalConf;
    private SourceCounter tableCounter;
    private String IPAddress;
    private String fromDBIP;
    private String entrySql;



    public EntryConverter(CanalConf canalConf, SourceCounter tableCounter) {
        this.canalConf = canalConf;
        this.tableCounter = tableCounter;
        IPAddress = Utility.getLocalIP(canalConf.getIpInterface());
        fromDBIP = canalConf.getFromDBIP();
    }


    public List<Event> convert(CanalEntry.Entry entry) {
        List<Event> events = new ArrayList<Event>();

        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND
                || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {

            numberInTransaction = 0L;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                CanalEntry.TransactionEnd end = null;
                try {
                    end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                } catch (InvalidProtocolBufferException e) {
                    LOGGER.warn("parse transaction end event has an error , data:" +  entry.toString());
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
            }
        }

        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOGGER.warn("parse row data event has an error , data:" + entry.toString(), e);
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            }
            CanalEntry.EventType eventType = rowChange.getEventType();

            // canal 在 QUERY 事件没有做表过滤
            if (eventType == CanalEntry.EventType.QUERY) {
                entrySql = rowChange.getSql();
            } else if (rowChange.getIsDdl()) {
                events.add(getSqlEvent(entry, rowChange.getSql()));
            } else {
                // 在每执行一次数据库数据更新操作前都会执行一次 QUERY 操作获取操作的sql语句
                events.add(getSqlEvent(entry, entrySql));

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                    String keyName = getTableKeyName(entry.getHeader());
                    String topic = canalConf.getTableTopic(keyName);
                    // 处理行数据
                    Map<String, String> eventData = handleRowData(rowData, entry.getHeader(),
                            eventType);
                    LOGGER.debug("eventData handleRowData:{}", eventData);
                    // 监控表数据
                    tableCounter.incrementTableReceivedCount(keyName);

                    String pk = getPK(rowData);
                    // 处理 event Header
                    LOGGER.debug("RowData pk:{}", pk);
                    Map<String, String> header = handleRowDataHeader(topic, pk);
                    events.add(dataToAvroEvent(eventData, header, topic));
                    numberInTransaction++;
                }
            }
        }

        return events;
    }

    /*
    * 获取 sql topic Event数据
    * */
    private Event getSqlEvent(CanalEntry.Entry entry, String sql) {
        Map<String, Object> eventSql = handleSQL(sql, entry.getHeader());
        Map<String, String> sqlHeader = Maps.newHashMap();
        sqlHeader.put("topic", "sql");
        return dataToJSONEvent(eventSql, sqlHeader);
    }

    /*
    * 将 data, header 转换为 JSON Event 格式
    * */
    private Event dataToJSONEvent(Map<String, Object> eventData, Map<String, String> eventHeader) {
        byte[] eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
        return EventBuilder.withBody(eventBody,eventHeader);
    }


    private  String getTableFieldSchema(List<String> schemaFieldList, String schemaName) {

        List<String> resultList = Lists.newArrayList();

        String schema = "{"
                + "\"type\":\"record\","
                + "\"name\":\""+ schemaName +"\","
                + "\"fields\":[";

        for (String fieldStr: schemaFieldList) {
            String field = "{ \"name\":\"" + fieldStr + "\", \"type\":\"string\" }";
            resultList.add(field);
        }
        schema += Joiner.on(",").join(resultList);
        schema += "]}";
        return schema;
    }

    /*
    * 将 data, header 转换为 Avro Event 格式
    * */
    private Event dataToAvroEvent(Map<String, String> eventData,
                                  Map<String, String> eventHeader, String topic) {

        List<String> schemaFieldList = this.canalConf.getTopicToSchemaFields().get(topic);
        List<String> attrList = Lists.newArrayList("__table", "__ts", "__db", "__type", "__agent", "__from");

        String schemaName = this.canalConf.getTopicToSchemaMap().get(topic);

        Schema.Parser parser = new Schema.Parser();

        String schemaString = getTableFieldSchema(ListUtils.union(schemaFieldList, attrList), schemaName);
        Schema schema = parser.parse(schemaString);

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        
        GenericData.Record avroRecord = new GenericData.Record(schema);

        for (String fieldStr: schemaFieldList) {
            String tableField = this.canalConf.getTopicSchemaFieldToTableField().get(topic, fieldStr);
            avroRecord.put(fieldStr, eventData.get(tableField));
        }

        for (String fieldStr: attrList) {
            avroRecord.put(fieldStr, eventData.get(fieldStr));
        }

        byte[] eventBody = recordInjection.apply(avroRecord);
        return EventBuilder.withBody(eventBody,eventHeader);
    }

    /*
    * 处理 sql topic 的数据格式
    * */
    private Map<String, Object> handleSQL(String sql, CanalEntry.Header entryHeader) {
        Map<String, Object> eventMap = Maps.newHashMap();
        eventMap.put("table", entryHeader.getTableName());
        eventMap.put("ts", Math.round(entryHeader.getExecuteTime() / 1000));
        eventMap.put("db", entryHeader.getSchemaName());
        eventMap.put("sql", sql);
        eventMap.put("agent", IPAddress);
        eventMap.put("from", fromDBIP);
        return eventMap;
    }

    /*
    * 处理行数据，并添加其他字段信息
    * */
    private Map<String, String> handleRowData(CanalEntry.RowData rowData,
                                       CanalEntry.Header entryHeader, CanalEntry.EventType eventType) {
        Map<String, String> eventMap = Maps.newHashMap();
        Map<String, String> rowDataMap;

        if (eventType == CanalEntry.EventType.DELETE) {
            // 删除事件 getAfterColumnsList 数据为空
            rowDataMap = convertColumnListToMap(rowData.getBeforeColumnsList(), entryHeader);
        } else {
            rowDataMap = convertColumnListToMap(rowData.getAfterColumnsList(), entryHeader);
        }
        /* 之前的数据
        if (canalConf.getOldDataRequired()) {
            Map<String, Object> beforeRowMap = convertColumnListToMap(rowData.getBeforeColumnsList(), entryHeader);
            eventMap.put("old", beforeRowMap);
        }
        */
        eventMap.put("__table", entryHeader.getTableName());
        eventMap.put("__ts", String.valueOf(Math.round(entryHeader.getExecuteTime() / 1000)));
        eventMap.put("__db", entryHeader.getSchemaName());
        eventMap.put("__type", eventType.toString());
        eventMap.put("__agent", IPAddress);
        eventMap.put("__from", fromDBIP);
        eventMap.putAll(rowDataMap);
        return  eventMap;
    }

    /*
    * 获取表的主键,用于kafka的分区key
    * */
    private String getPK(CanalEntry.RowData rowData) {
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

    private String getTableKeyName(CanalEntry.Header entryHeader) {
        String table = entryHeader.getTableName();
        String database = entryHeader.getSchemaName();
        return database + '.' + table;
    }

    /*
    * 处理 Event Header 获取数据的 topic
    * */
    private Map<String, String> handleRowDataHeader(String topic, String kafkaKey) {

        Map<String, String> header = Maps.newHashMap();
        if (kafkaKey != null){
            // 将表的主键作为kafka分区的key
            header.put("key", kafkaKey);
        }
        header.put("topic", topic);
        header.put("numInTransaction", String.valueOf(numberInTransaction));
        return header;
    }

    /*
    * 对列数据进行解析
    * */
    private Map<String, String> convertColumnListToMap(List<CanalEntry.Column> columns, CanalEntry.Header entryHeader) {
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
