package com.citic.source.canal.resolve;

import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER;
import static com.citic.sink.canal.KafkaSinkConstants.KEY_HEADER;
import static com.citic.sink.canal.KafkaSinkConstants.SCHEMA_NAME;
import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;
import static com.citic.source.canal.CanalSourceConstants.GSON;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_AGENT;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_DB;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_FROM;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TABLE;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TS;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TYPE;
import static com.citic.source.canal.CanalSourceConstants.SUPPORT_TIME_FORMAT;
import static com.citic.source.canal.CanalSourceConstants.TOKEN_TYPE;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.AgentCounter;
import com.citic.helper.FlowCounter;
import com.citic.helper.SchemaCache;
import com.citic.source.canal.CanalConf;
import com.citic.source.canal.core.AbstractCommonDataHandler;
import com.citic.source.canal.core.DataHandlerInterface;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.ListUtils;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractDataHandler extends AbstractCommonDataHandler implements
    DataHandlerInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataHandler.class);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat(SUPPORT_TIME_FORMAT);

    private final CanalConf canalConf;
    private final List<String> attrList;

    private AbstractDataHandler(CanalConf canalConf) {
        this.canalConf = canalConf;

        attrList = Lists.newArrayList(META_FIELD_DB, META_FIELD_TABLE, META_FIELD_AGENT,
            META_FIELD_FROM, META_FIELD_TS, META_FIELD_TYPE);
    }

    /*
     * 获取表的主键,用于kafka的分区key
     * */
    private static String getPk(CanalEntry.RowData rowData) {
        StringBuilder pk = null;
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            if (column.getIsKey()) {
                if (pk == null) {
                    pk = new StringBuilder();
                }
                pk.append(column.getValue());
            }
        }
        if (pk == null) {
            return null;
        } else {
            return pk.toString();
        }
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
        CanalEntry.EventType eventType) {
        String keyName = getTableKeyName(entryHeader);
        String topic = canalConf.getTableTopic(keyName);

        if (Strings.isNullOrEmpty(topic)) {
            return null;
        }

        // 处理行数据
        Map<String, String> eventData = handleRowData(rowData, entryHeader, eventType);
        LOGGER.debug("eventData handleRowData:{}", eventData);

        if (!canalConf.isShutdownFlowCounter()) {
            doDataCount(topic, keyName, eventData, eventType, entryHeader);
        }

        String pk = getPk(rowData);
        // 处理 event Header
        LOGGER.debug("RowData pk:{}", pk);
        Map<String, String> header = handleRowDataHeader(topic, pk);

        return dataToEvent(eventData, header, topic);
    }

    private void doDataCount(String topic, String keyName, Map<String, String> eventData,
        CanalEntry.EventType eventType, CanalEntry.Header entryHeader) {
        // topic 数据量统计
        String timeFieldName = this.getTimeFieldName(topic);
        if (timeFieldName != null) {
            String timeFieldValue;
            if (eventType == CanalEntry.EventType.DELETE) {
                // 删除数据，通过 binlog 执行时间进行统计
                timeFieldValue = dateFormat.format(new Date(entryHeader.getExecuteTime()));
            } else {
                timeFieldValue = eventData.get(timeFieldName);
            }
            FlowCounter.increment(topic, keyName, canalConf.getFromDbIp(), timeFieldValue);
        }
        // agent 数据量统计
        AgentCounter.increment(canalConf.getAgentIpAddress());
    }

    abstract Event dataToEvent(Map<String, String> eventData,
        Map<String, String> eventHeader,
        String topic);

    abstract void splitTableToTopicMap(String tableToTopicMap);

    abstract void splitTableFieldsFilter(String tableFieldsFilter);

    abstract String getTimeFieldName(String topic);


    /*
     * 处理 Event Header 获取数据的 topic
     * */
    private Map<String, String> handleRowDataHeader(String topic, String kafkaKey) {
        Map<String, String> header = Maps.newHashMap();
        if (kafkaKey != null) {
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
        CanalEntry.EventType eventType) {
        Map<String, String> eventMap = Maps.newHashMap();
        Map<String, String> rowDataMap;

        if (eventType == CanalEntry.EventType.DELETE) {
            // 删除事件 getAfterColumnsList 数据为空
            rowDataMap = convertColumnListToMap(rowData.getBeforeColumnsList(), entryHeader);
        } else {
            rowDataMap = convertColumnListToMap(rowData.getAfterColumnsList(), entryHeader);
        }

        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, DECIMAL_FORMAT_3.format(System.currentTimeMillis() / 1000.0));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_TYPE, eventType.toString());
        eventMap.put(META_FIELD_AGENT, canalConf.getAgentIpAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDbIp());

        eventMap.putAll(rowDataMap);
        return eventMap;
    }


    static class Avro extends AbstractDataHandler {

        // topic list
        private final List<String> topicAppendList = Lists.newArrayList();

        // topic -> schema
        private final Map<String, String> topicToSchemaMap = Maps.newHashMap();

        // topic -> firstSchemaField
        private final Map<String, String> topicToFirstSchemaField = Maps.newHashMap();

        // topic -> schema fields list
        private final Map<String, Set<String>> topicToSchemaFields = Maps.newHashMap();
        // topic,schema_field -> table_field
        private final Table<String, String, String> topicSchemaFieldToTableField = HashBasedTable
            .create();


        Avro(CanalConf canalConf) {
            super(canalConf);

            splitTableToTopicMap(canalConf.getTableToTopicMap());
            splitTableFieldsFilter(canalConf.getTableFieldsFilter());
        }

        void splitTableToTopicMap(String tableToTopicMap) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tableToTopicMap),
                "tableToTopicMap cannot empty");
            // test.test:test123:schema1;test.test1:test234:schema2
            Splitter.on(';')
                .omitEmptyStrings()
                .trimResults()
                .split(tableToTopicMap)
                .forEach(item -> {
                    String[] result = item.split(":");

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
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tableFieldsFilter),
                "tableFieldsFilter cannot empty");
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

                    Set<String> schemaFields = Sets.newLinkedHashSet();
                    final String[] firstSchemaField = {null};
                    Splitter.on(",")
                        .omitEmptyStrings()
                        .trimResults()
                        .split(item)
                        .forEach(field -> {
                            String[] fieldTableSchema = field.split("\\|");
                            Preconditions.checkArgument(fieldTableSchema.length == 2,
                                "tableFieldsFilter 格式错误 eg: id|id1,name|name1");

                            Preconditions
                                .checkArgument(!Strings.isNullOrEmpty(fieldTableSchema[0].trim()),
                                    "table field cannot empty");
                            Preconditions
                                .checkArgument(!Strings.isNullOrEmpty(fieldTableSchema[1].trim()),
                                    "schema field cannot empty");

                            if (firstSchemaField[0] == null) {
                                firstSchemaField[0] = fieldTableSchema[1];
                            }

                            // 更新时间字段在字段列表中的顺序
                            if (schemaFields.contains(fieldTableSchema[1])) {
                                schemaFields.remove(fieldTableSchema[1]);
                                schemaFields.add(fieldTableSchema[1]);
                            } else {
                                schemaFields.add(fieldTableSchema[1]);
                            }

                            this.topicSchemaFieldToTableField
                                .put(topic, fieldTableSchema[1], fieldTableSchema[0]);

                        });

                    if (firstSchemaField[0] != null) {
                        topicToFirstSchemaField.put(topic, firstSchemaField[0]);
                    }

                    topicToSchemaFields.put(topic, schemaFields);
                });
        }

        @Override
        String getTimeFieldName(String topic) {
            return topicToFirstSchemaField.get(topic);
        }

        /*
         * 将 data, header 转换为 Avro Event 格式
         * */
        @Override
        Event dataToEvent(Map<String, String> eventData,
            Map<String, String> eventHeader,
            String topic) {

            Set<String> schemaFieldList = topicToSchemaFields.get(topic);
            if (schemaFieldList == null || schemaFieldList.size() == 0) {
                return null;
            }

            String schemaName = topicToSchemaMap.get(topic);
            Schema schema = SchemaCache
                .getSchema2(schemaFieldList, super.attrList, schemaName);

            GenericRecord avroRecord = new GenericData.Record(schema);
            for (String fieldStr : schemaFieldList) {
                String tableField = topicSchemaFieldToTableField.get(topic, fieldStr);
                avroRecord.put(fieldStr, eventData.getOrDefault(tableField, ""));
            }

            for (String fieldStr : super.attrList) {
                avroRecord.put(fieldStr, eventData.getOrDefault(fieldStr, ""));
            }

            // 用于sink解析
            eventHeader.put(SCHEMA_NAME, schemaName);
            LOGGER.debug("event data: {}", avroRecord);
            LOGGER.debug("event header: {}", eventHeader);

            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
            byte[] eventBody = recordInjection.apply(avroRecord);
            return EventBuilder.withBody(eventBody, eventHeader);
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
                List<String> unionList = ListUtils.union(tableFields, super.attrList);
                unionList.forEach(fieldName -> {
                    filterTableData.put((String) fieldName, eventData.getOrDefault(fieldName, ""));
                });
                eventBody = GSON.toJson(filterTableData, TOKEN_TYPE)
                    .getBytes(Charset.forName("UTF-8"));

                LOGGER.debug("event data: {}", filterTableData);
            } else {
                eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
                LOGGER.debug("event data: {}", eventData);
            }

            LOGGER.debug("event header: {}", eventHeader);
            return EventBuilder.withBody(eventBody, eventHeader);
        }

        @Override
        void splitTableToTopicMap(String tableToTopicMap) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tableToTopicMap),
                "tableToTopicMap cannot empty");
            // test.test:test123;test.test1:test234
            Splitter.on(';')
                .omitEmptyStrings()
                .trimResults()
                .split(tableToTopicMap)
                .forEach(item -> {
                    String[] result = item.split(":");
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
            if (Strings.isNullOrEmpty(tableFieldsFilter)) {
                return;
            }

            final int[] counter = {0};
            Splitter.on(';')
                .omitEmptyStrings()
                .trimResults()
                .split(tableFieldsFilter)
                .forEach(item -> {
                    String topic = topicAppendList.get(counter[0]);
                    counter[0] += 1;

                    Iterable<String> fieldsList = Splitter.on(",")
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
            if (tableFields == null || tableFields.size() == 0) {
                return null;
            } else {
                // 默认首个字段为时间字段
                return tableFields.get(0);
            }

        }
    }

}
