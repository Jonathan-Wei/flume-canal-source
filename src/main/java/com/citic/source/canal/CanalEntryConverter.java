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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CanalEntryConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalEntryConverter.class);
    private static String IPAddress;

    private static final Gson gson = new Gson();
    private static Long numberInTransaction = 0L;

    private static CanalConf canalConf;
    private static SourceCounter tableCounter;

    public static void setCanalConf(CanalConf argCanalConf) {
        canalConf = argCanalConf;
        IPAddress = Utility.getLocalIP(canalConf.getIpInterface());
    }

    public static void setTableCounter(SourceCounter argTableCounter) {
        tableCounter = argTableCounter;
    }

    public static List<Event> convert(CanalEntry.Entry entry) {
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

            if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
                // TODO get sql
            } else {

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                    // 处理行数据
                    Map<String, Object> eventMap = handleRowData(rowData, entry.getHeader(),
                            eventType.toString());

                    // 监控表数据
                    tableCounter.incrementTableReceivedCount(getTableKeyName(entry.getHeader()));

                    byte[] eventBody = gson.toJson(eventMap, new TypeToken<Map<String, Object>>(){}.getType())
                            .getBytes(Charset.forName("UTF-8"));


                    String pk = getPK(rowData);
                    // 处理 event Header
                    LOGGER.debug("rowdata pk:{}", pk);
                    Map<String, String> header = handleHeader(entry.getHeader(), pk);

                    events.add(EventBuilder.withBody(eventBody,header));
                    numberInTransaction++;
                }
            }
        }

        return events;
    }

    /*
    * 处理行数据，并添加其他字段信息
    * */
    private static Map<String, Object> handleRowData(CanalEntry.RowData rowData,
                                       CanalEntry.Header entryHeader, String eventType) {
        Map<String, Object> eventMap = new HashMap<String, Object>();

        Map<String, Object> rowMap = convertColumnListToMap(rowData.getAfterColumnsList(),
                                                            canalConf, entryHeader);
        if (canalConf.getOldDataRequired()) {
            Map<String, Object> beforeRowMap = convertColumnListToMap(rowData.getBeforeColumnsList(),
                                                                      canalConf, entryHeader);
            eventMap.put("old", beforeRowMap);
        }

        eventMap.put("table", entryHeader.getTableName());
        eventMap.put("ts", Math.round(entryHeader.getExecuteTime() / 1000));
        eventMap.put("db", entryHeader.getSchemaName());
        eventMap.put("data", rowMap);
        eventMap.put("type", eventType);
        eventMap.put("agent", IPAddress);
        return  eventMap;
    }

    /*
    * 获取表的主键,用于kafka的分区key
    * */
    private static String getPK(CanalEntry.RowData rowData) {
        String pk = null;
        for(CanalEntry.Column column : rowData.getAfterColumnsList()) {
            if (column.getIsKey()) {
                if (pk == null) {
                    pk = "";
                }
                pk += column.getValue();
            }
        }
        return pk;
    }

    private static String getTableKeyName(CanalEntry.Header entryHeader) {
        String table = entryHeader.getTableName();
        String database = entryHeader.getSchemaName();
        return database + '.' + table;
    }

    /*
    * 处理 Event Header 获取数据的 topic
    * */
    private static Map<String, String> handleHeader(CanalEntry.Header entryHeader, String kafkaKey) {
        String keyName = getTableKeyName(entryHeader);
        String topic = canalConf.getTableTopic(keyName);

        Map<String, String> header = new HashMap<String, String>();
        if (kafkaKey != null){
            // 将表的主键作为kafka分区的key
            header.put("key", kafkaKey);
        }
        header.put("topic", topic);
        header.put("numInTransaction", String.valueOf(CanalEntryConverter.numberInTransaction));
        return header;
    }

    /*
    * 对列数据进行解析
    * */
    private static Map<String, Object> convertColumnListToMap(List<CanalEntry.Column> columns,
                                                              CanalConf canalConf,
                                                              CanalEntry.Header entryHeader) {
        Map<String, Object> rowMap = new HashMap<String, Object>();

        String keyName = entryHeader.getSchemaName() + '.' + entryHeader.getTableName();

        for(CanalEntry.Column column : columns) {
            int sqlType = column.getSqlType();

            // 根据配置做字段过滤
            if (!canalConf.isFieldNeedOutput(keyName, column.getName())) {
                continue;
            }

            String stringValue = column.getValue();
            Object colValue;

            try {
                switch (sqlType) {
                    case Types.NULL: {
                        colValue = stringValue;
                        LOGGER.warn("JDBC type {} not currently supported", sqlType);
                        break;
                    }

                    case Types.BOOLEAN: {
                        colValue = Boolean.parseBoolean(stringValue);
                        break;
                    }

                    // ints <= 8 bits
                    case Types.BIT:
                    case Types.TINYINT: {
                        colValue = new Integer(stringValue).byteValue();
                        break;
                    }
                    // 16 bit ints
                    case Types.SMALLINT: {
                        colValue = Short.parseShort(stringValue);
                        break;
                    }

                    // 32 bit ints
                    case Types.INTEGER: {
                        colValue = Integer.parseInt(stringValue);
                        break;
                    }

                    // 64 bit ints
                    case Types.BIGINT: {
                        colValue = Long.parseLong(stringValue);
                        break;
                    }

                    // REAL is a single precision floating point value, i.e. a Java float
                    case Types.REAL: {
                        colValue = Float.parseFloat(stringValue);
                        break;
                    }

                    // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
                    // for single precision
                    case Types.FLOAT:
                    case Types.DOUBLE: {
                        colValue = Double.parseDouble(stringValue);
                        break;
                    }

                    case Types.NUMERIC:
                    case Types.DECIMAL: {
                        colValue = stringValue;
                        break;
                    }

                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                    case Types.DATALINK:

                        // Binary == fixed bytes
                        // BLOB, VARBINARY, LONGVARBINARY == bytes
                    case Types.BINARY:
                    case Types.BLOB:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY: {
                        colValue = stringValue;
                        break;
                    }

                    // Date is day + moth + year
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP: {
                        colValue = stringValue;
                        break;
                    }

                    case Types.ARRAY:
                    case Types.JAVA_OBJECT:
                    case Types.OTHER:
                    case Types.DISTINCT:
                    case Types.STRUCT:
                    case Types.REF:
                    default: {
                        LOGGER.warn("JDBC type {} not currently supported", sqlType);
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
