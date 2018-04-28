package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.AgentCounter;
import com.citic.helper.FlowCounter;
import com.citic.helper.SchemaCache;
import com.citic.helper.Utility;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
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

import java.nio.charset.Charset;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static com.citic.sink.canal.KafkaSinkConstants.*;
import static com.citic.source.canal.CanalSourceConstants.*;

interface DataHandlerInterface {
    Event getDataEvent(CanalEntry.RowData rowData,
                       CanalEntry.Header entryHeader,
                       CanalEntry.EventType eventType,
                       String sql);
}

abstract class AbstractDataHandler implements DataHandlerInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataHandler.class);

    private final CanalConf canalConf;
    private final List<String> attr_list;

    private AbstractDataHandler(CanalConf canalConf) {
        this.canalConf = canalConf;

        attr_list = Lists.newArrayList(META_FIELD_TABLE, META_FIELD_TS,
                META_FIELD_DB, META_FIELD_TYPE, META_FIELD_AGENT, META_FIELD_FROM);

        if (canalConf.isWriteSQLToData()) {
            attr_list.add(META_FIELD_SQL);
        }
    }

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
    public Event getDataEvent(CanalEntry.RowData rowData,
                              CanalEntry.Header entryHeader,
                              CanalEntry.EventType eventType,
                              String sql) {
        String keyName = getTableKeyName(entryHeader);
        String topic = canalConf.getTableTopic(keyName);
        // 处理行数据
        Map<String, String> eventData = handleRowData(rowData, entryHeader, eventType, sql);
        LOGGER.debug("eventData handleRowData:{}", eventData);

        if (!canalConf.isShutdownFlowCounter()) {
            // topic 数据量统计
            String timeFieldName = this.getTimeFieldName(topic);
            if (timeFieldName != null) {
                String timeFieldValue = eventData.get(timeFieldName);
                FlowCounter.increment(topic, keyName, canalConf.getFromDBIP(), timeFieldValue);
            }
            // agent 数据量统计
            AgentCounter.increment(canalConf.getAgentIPAddress());
        }

        String pk = getPK(rowData);
        // 处理 event Header
        LOGGER.debug("RowData pk:{}", pk);
        Map<String, String> header = handleRowDataHeader(topic, pk);

        return dataToEvent(eventData, header, topic);
    }

    abstract Event dataToEvent(Map<String, String> eventData,
                                  Map<String, String> eventHeader,
                                  String topic);

    abstract void splitTableToTopicMap(String tableToTopicMap);

    abstract void splitTableFieldsFilter(String tableFieldsFilter);

    abstract String getTimeFieldName (String topic);


    /*
    * 处理 Event Header 获取数据的 topic
    * */
    private Map<String, String> handleRowDataHeader(String topic, String kafkaKey) {
        Map<String, String> header = Maps.newHashMap();
        if (kafkaKey != null){
            // 将表的主键作为kafka分区的key
            header.put(KEY_HEADER, kafkaKey);
        }
        header.put(DEFAULT_TOPIC_OVERRIDE_HEADER, topic);
        return header;
    }

    /*
    * 处理行数据，并添加其他字段信息
    * */
    private Map<String, String> handleRowData(CanalEntry.RowData rowData,
                                              CanalEntry.Header entryHeader,
                                              CanalEntry.EventType eventType,
                                              String sql) {
        Map<String, String> eventMap = Maps.newHashMap();
        Map<String, String> rowDataMap;

        if (eventType == CanalEntry.EventType.DELETE) {
            // 删除事件 getAfterColumnsList 数据为空
            rowDataMap = convertColumnListToMap(rowData.getBeforeColumnsList(), entryHeader);
        } else {
            rowDataMap = convertColumnListToMap(rowData.getAfterColumnsList(), entryHeader);
        }

        if (canalConf.isWriteSQLToData()) {
            eventMap.put(META_FIELD_SQL, sql);
        }
        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, String.valueOf(System.currentTimeMillis()));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_TYPE, eventType.toString());
        eventMap.put(META_FIELD_AGENT, canalConf.getAgentIPAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDBIP());

        eventMap.putAll(rowDataMap);
        return  eventMap;
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
                    * mysql datetime maps to a java.sql.Timestamp
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

    static class Avro extends AbstractDataHandler {
        // topic list
        private final List<String> topicAppendList = Lists.newArrayList();

        // topic -> schema
        private final Map<String, String> topicToSchemaMap = Maps.newHashMap();

        // topic -> schema fields list
        private final Map<String, List<String>> topicToSchemaFields = Maps.newHashMap();
        // topic,schema_field -> table_field
        private final Table<String, String, String> topicSchemaFieldToTableField = HashBasedTable.create();


        Avro(CanalConf canalConf) {
            super(canalConf);

            splitTableToTopicMap(canalConf.getTableToTopicMap());
            splitTableFieldsFilter(canalConf.getTableFieldsFilter());
        }

         void splitTableToTopicMap(String tableToTopicMap) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tableToTopicMap), "tableToTopicMap cannot empty");
            // test.test:test123:schema1;test.test1:test234:schema2
            Splitter.on(';')
                    .omitEmptyStrings()
                    .trimResults()
                    .split(tableToTopicMap)
                    .forEach(item ->{
                        String[] result =  item.split(":");

                        Preconditions.checkArgument(result.length == 3,
                                "tableToTopicMap format incorrect eg: db.tbl1:topic1:schema1");
                        Preconditions.checkArgument(!Strings.isNullOrEmpty(result[0].trim()),
                                "db.table cannot empty");
                        Preconditions.checkArgument(!Strings.isNullOrEmpty(result[1].trim()),
                                "topic cannot empty");
                        Preconditions.checkArgument(!Strings.isNullOrEmpty(result[2].trim()),
                                "schema cannot empty");

                        // result[1] == topic, result[2] == schema
                        topicAppendList.add(result[1].trim());
                        // topic -> avro schema
                        this.topicToSchemaMap.put(result[1].trim(), result[2].trim());
                    });
        }

        /*
        * 设置表名与字段过滤对应 table
        * */
        @Override
         void splitTableFieldsFilter(String tableFieldsFilter) {
             Preconditions.checkArgument(!Strings.isNullOrEmpty(tableFieldsFilter), "tableFieldsFilter cannot empty");
             // 这里表的顺序根据配置文件中 tableToTopicRegexMap 表的顺序
            // id|id1,name|name1;uid|uid2,name|name2
            final int[] counter = {0};
            Splitter.on(';')
                    .omitEmptyStrings()
                    .trimResults()
                    .split(tableFieldsFilter)
                    .forEach(item -> {
                        String topic = topicAppendList.get(counter[0]);
                        counter[0] += 1;

                        List<String> schemaFields = Lists.newArrayList();
                        Splitter.on(",")
                                .omitEmptyStrings()
                                .trimResults()
                                .split(item)
                                .forEach(field -> {
                                    String[] fieldTableSchema = field.split("\\|");
                                    Preconditions.checkArgument(fieldTableSchema.length == 2,
                                            "tableFieldsFilter 格式错误 eg: id|id1,name|name1");

                                    Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldTableSchema[0].trim()),
                                            "table field cannot empty");
                                    Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldTableSchema[1].trim()),
                                            "schema field cannot empty");

                                    schemaFields.add(fieldTableSchema[1]);
                                    this.topicSchemaFieldToTableField.put(topic, fieldTableSchema[1], fieldTableSchema[0]);

                                });
                        topicToSchemaFields.put(topic, schemaFields);
                    });
        }

        @Override
        String getTimeFieldName(String topic) {
            List<String> schemaFields =  topicToSchemaFields.get(topic);
            if (schemaFields == null || schemaFields.size() == 0)
                return null;
            // 默认首个字段为时间字段
            return this.topicSchemaFieldToTableField.get(topic, schemaFields.get(0));
        }


        /*
        * 将 data, header 转换为 Avro Event 格式
        * */
        @Override
        Event dataToEvent(Map<String, String> eventData,
                                      Map<String, String> eventHeader,
                                      String topic) {

            List<String> schemaFieldList  = topicToSchemaFields.get(topic);
            String schemaName = topicToSchemaMap.get(topic);

            // schemaFieldList and ATTR_LIST are same List<String> type
            @SuppressWarnings("unchecked")
            String schemaString = Utility.getTableFieldSchema(ListUtils.union(schemaFieldList, super.attr_list), schemaName);
            Schema schema = SchemaCache.getSchema(schemaString);

            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
            GenericRecord avroRecord = new GenericData.Record(schema);

            for (String fieldStr: schemaFieldList) {
                String tableField = topicSchemaFieldToTableField.get(topic, fieldStr);
                avroRecord.put(fieldStr, eventData.getOrDefault(tableField, ""));
            }

            for (String fieldStr: super.attr_list) {
                avroRecord.put(fieldStr, eventData.getOrDefault(fieldStr, ""));
            }

            byte[] eventBody = recordInjection.apply(avroRecord);

            // 用于sink解析
            eventHeader.put(SCHEMA_HEADER, schemaString);
            LOGGER.debug("event data: {}", avroRecord);
            LOGGER.debug("event header: {}", eventHeader);

            return EventBuilder.withBody(eventBody,eventHeader);
        }
    }

    static class Json extends AbstractDataHandler {
        // topic list
        private final List<String> topicAppendList = Lists.newArrayList();

        // topic -> table fields list
        private final Map<String, List<String>> topicToTableFields = Maps.newHashMap();

        Json(CanalConf canalConf) {
            super(canalConf);
            splitTableToTopicMap(canalConf.getTableToTopicMap());
            splitTableFieldsFilter(canalConf.getTableFieldsFilter());
        }

        @Override
        Event dataToEvent(Map<String, String> eventData, Map<String, String> eventHeader,
                          String topic) {

            List<String> tableFields = topicToTableFields.get(topic);
            byte[] eventBody;

            if (tableFields != null && tableFields.size() > 0) {
                Map<String, String> filterTableData = Maps.newHashMap();

                // schemaFieldList and ATTR_LIST are same List<String> type
                @SuppressWarnings("unchecked")
                List<String> unionList = ListUtils.union(tableFields, super.attr_list);
                unionList.forEach(fieldName -> {
                    filterTableData.put((String) fieldName, eventData.getOrDefault(fieldName,""));
                });
                eventBody = GSON.toJson(filterTableData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));

                LOGGER.debug("event data: {}", filterTableData);
            } else {
                eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
                LOGGER.debug("event data: {}", eventData);
            }

            LOGGER.debug("event header: {}", eventHeader);
            return EventBuilder.withBody(eventBody,eventHeader);
        }

        @Override
        void splitTableToTopicMap(String tableToTopicMap) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tableToTopicMap), "tableToTopicMap cannot empty");
            // test.test:test123;test.test1:test234
            Splitter.on(';')
                    .omitEmptyStrings()
                    .trimResults()
                    .split(tableToTopicMap)
                    .forEach(item ->{
                        String[] result =  item.split(":");
                        topicAppendList.add(result[1].trim());
                    });
        }

        @Override
        void splitTableFieldsFilter(String tableFieldsFilter) {
            /*
            * tableFieldsFilter 这里的值可以空,空表示不对字段进行过滤
            * 这里表的顺序根据配置文件中 tableToTopicMap 表的顺序
            * id,name;uid,name
            * */
            if (Strings.isNullOrEmpty(tableFieldsFilter))
                return;

            final int[] counter = {0};
            Splitter.on(';')
                    .omitEmptyStrings()
                    .trimResults()
                    .split(tableFieldsFilter)
                    .forEach(item -> {
                        String topic = topicAppendList.get(counter[0]);
                        counter[0] += 1;

                        Iterable<String> fieldsList =  Splitter.on(",")
                                .omitEmptyStrings()
                                .trimResults()
                                .split(item);
                        List<String> tableFields = Lists.newArrayList(fieldsList);
                        topicToTableFields.put(topic, tableFields);
                    });
        }

        @Override
        String getTimeFieldName(String topic) {
            List<String> tableFields = topicToTableFields.get(topic);
            if (tableFields == null || tableFields.size() == 0)
                return null;
            else
                // 默认首个字段为时间字段
                return tableFields.get(0);

        }
    }

}
