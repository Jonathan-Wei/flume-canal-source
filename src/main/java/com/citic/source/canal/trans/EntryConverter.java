
package com.citic.source.canal.trans;

import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER;
import static com.citic.sink.canal.KafkaSinkConstants.SCHEMA_NAME;
import static com.citic.source.canal.CanalSourceConstants.GSON;
import static com.citic.source.canal.CanalSourceConstants.META_DATA;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_AGENT;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_FROM;
import static com.citic.source.canal.CanalSourceConstants.META_TRANS_ID;
import static com.citic.source.canal.CanalSourceConstants.TOKEN_TYPE;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.SchemaCache;
import com.citic.source.canal.AbstractEntrySqlHandler.Avro;
import com.citic.source.canal.CanalConf;
import com.citic.source.canal.core.EntryConverterInterface;
import com.citic.source.canal.core.EntrySqlHandlerInterface;
import com.citic.source.canal.core.TransDataHandlerInterface;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EntryConverter implements EntryConverterInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntryConverter.class);
    private static final Type LIST_TOKEN_TYPE = new TypeToken<List<String>>() {}.getType();

    private final EntrySqlHandlerInterface sqlHandler;
    private final TransDataHandlerInterface dataHandler;
    private final List<String> transDataList;
    private final CanalConf canalConf;

    // topic -> schema
    private final Map<String, String> topicToSchemaMap = Maps.newHashMap();
    private String transId = null;
    private String normalSql;

    private final List<String> attrList;

    public EntryConverter(CanalConf canalConf) {
        this.canalConf = canalConf;
        this.sqlHandler = new Avro();
        this.dataHandler = new AbstractDataHandler(canalConf);
        this.transDataList = Lists.newArrayList();

        this.attrList = Lists
            .newArrayList(META_DATA, META_TRANS_ID, META_FIELD_AGENT, META_FIELD_FROM);

        splitTableToTopicMap(canalConf.getTableToTopicMap());

    }

    /*
     * 处理 Event Header 获取数据的 topic
     * */
    private Map<String, String> handleRowDataHeader(String topic) {
        Map<String, String> header = Maps.newHashMap();
        header.put(DEFAULT_TOPIC_OVERRIDE_HEADER, topic);
        return header;
    }

    /*
     * 处理行数据，并添加其他字段信息
     * */
    private Map<String, String> handleRowData(String transListData) {
        Map<String, String> eventMap = Maps.newHashMap();

        eventMap.put(META_DATA, transListData);
        eventMap.put(META_TRANS_ID, transId == null ? "" : transId);
        eventMap.put(META_FIELD_AGENT, canalConf.getAgentIpAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDbIp());
        return eventMap;
    }

    private void splitTableToTopicMap(String tableToTopicMap) {
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

                // topic -> avro schema
                this.topicToSchemaMap.put(result[1].trim(), result[2].trim());
            });
    }

    private Event dataToEvent(Map<String, String> eventData,
        Map<String, String> eventHeader,
        String topic) {

        String schemaName = topicToSchemaMap.get(topic);
        Schema schema = SchemaCache.getSchema(this.attrList, schemaName);

        GenericRecord avroRecord = new GenericData.Record(schema);

        for (String fieldStr : this.attrList) {
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

    private Event transDataToEvent() {
        String transListData = GSON.toJson(this.transDataList, LIST_TOKEN_TYPE);

        // 处理行数据
        Map<String, String> eventData = handleRowData(transListData);

        // 全局事务封装只能配置全库全表
        String allTables = canalConf.getFilterTableList().get(0);
        String topic = canalConf.getTableTopic(allTables);

        Map<String, String> header = handleRowDataHeader(topic);

        return dataToEvent(eventData, header, topic);
    }

    @Override
    public List<Event> convert(CanalEntry.Entry entry, CanalConf canalConf) {
        List<Event> events = new ArrayList<>();
        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
            CanalEntry.TransactionBegin begin = null;
            try {
                begin = CanalEntry.TransactionBegin
                    .parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error(
                    "parse transaction begin event has an error , data:" + entry.toString());
                throw new RuntimeException(
                    "parse event has an error , data:" + entry.toString(), e);
            }

            transId = begin.getTransactionId();
            this.transDataList.clear();
            LOGGER.debug("TRANSACTIONBEGIN transId:{}", transId);

        } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
            CanalEntry.TransactionEnd end = null;
            try {
                end = CanalEntry.TransactionEnd
                    .parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error(
                    "parse transaction end event has an error , data:" + entry.toString());
                throw new RuntimeException(
                    "parse event has an error , data:" + entry.toString(), e);
            }

            if (this.transDataList.size() > 0) {
                Event transEvent = transDataToEvent();
                events.add(transEvent);
                this.transDataList.clear();
            }
            LOGGER.debug("TRANSACTIONEND transId:{}", end.getTransactionId());
        } else if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOGGER.warn("parse row data event has an error , data:" + entry.toString(), e);
                throw new RuntimeException("parse event has an error , data:" + entry.toString(),
                    e);
            }
            CanalEntry.EventType eventType = rowChange.getEventType();
            CanalEntry.Header eventHeader = entry.getHeader();

            // canal 在 QUERY 事件没有做表过滤
            if (eventType == CanalEntry.EventType.QUERY) {
                normalSql = rowChange.getSql();
            } else if (rowChange.getIsDdl()) {
                // 只有 ddl 操作才记录 sql, 其他 insert update delete 不做sql记录操作
                events.add(this.sqlHandler.getSqlEvent(eventHeader, rowChange.getSql(), canalConf));
            } else {
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    String dataJsonString = this.dataHandler
                        .getDataJsonString(rowData, eventHeader, eventType);
                    if (dataJsonString != null) {
                        transDataList.add(dataJsonString);
                    }
                }
            }
        }
        return events;
    }
}
