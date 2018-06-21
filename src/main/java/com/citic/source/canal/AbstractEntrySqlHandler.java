package com.citic.source.canal;

import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER;
import static com.citic.sink.canal.KafkaSinkConstants.SCHEMA_NAME;
import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_AGENT;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_DB;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_FROM;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_SQL;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TABLE;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TS;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.SchemaCache;
import com.citic.source.canal.core.EntrySqlHandlerInterface;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;


/**
 * The type Abstract entry sql handler.
 */
public abstract class AbstractEntrySqlHandler implements EntrySqlHandlerInterface {

    private static final List<String> ATTR_LIST = Lists
        .newArrayList(META_FIELD_TABLE, META_FIELD_TS,
            META_FIELD_DB, META_FIELD_AGENT, META_FIELD_FROM, META_FIELD_SQL);
    private final String sqlTopicName;

    private AbstractEntrySqlHandler(String sqlTopicName) {
        this.sqlTopicName = sqlTopicName;
    }


    /**
     * change sql string to Event.
     *
     * @param entryHeader the event header
     * @param sql sql string
     * @param canalConf canal config
     * @return the event
     */
    public Event getSqlEvent(CanalEntry.Header entryHeader, String sql, CanalConf canalConf) {
        Map<String, String> eventSql = handleSql(sql, entryHeader, canalConf);
        Map<String, String> sqlHeader = Maps.newHashMap();
        sqlHeader.put(DEFAULT_TOPIC_OVERRIDE_HEADER, sqlTopicName);
        return dataToSqlEvent(eventSql, sqlHeader);
    }

    /**
     * Data to sql event event.
     *
     * @param eventData the event data
     * @param eventHeader the event header
     * @return the event
     */
    abstract Event dataToSqlEvent(Map<String, String> eventData, Map<String, String> eventHeader);

    /*
     * 处理 sql topic 的数据格式
     * */
    private Map<String, String> handleSql(String sql, CanalEntry.Header entryHeader,
        CanalConf canalConf) {
        Map<String, String> eventMap = Maps.newHashMap();
        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, DECIMAL_FORMAT_3.format(System.currentTimeMillis() / 1000.0));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_AGENT, canalConf.getAgentIpAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDbIp());
        eventMap.put(META_FIELD_SQL, Strings.isNullOrEmpty(sql) ? "no sql" : sql);
        return eventMap;
    }

    /**
     * The type Avro.
     */
    public static class Avro extends AbstractEntrySqlHandler {

        private static final String AVRO_SQL_TOPIC = "avro_ddl_sql";

        /**
         * Instantiates a new Avro.
         */
        public Avro() {
            super(AVRO_SQL_TOPIC);
        }

        /*
         * 将 data, header 转换为 JSON Event 格式
         * */
        Event dataToSqlEvent(Map<String, String> eventData, Map<String, String> eventHeader) {
            Schema schema = SchemaCache.getSchema(ATTR_LIST, AVRO_SQL_TOPIC);
            GenericRecord avroRecord = new GenericData.Record(schema);

            for (String fieldStr : ATTR_LIST) {
                avroRecord.put(fieldStr, eventData.getOrDefault(fieldStr, ""));
            }

            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
            byte[] eventBody = recordInjection.apply(avroRecord);

            // 用于sink解析
            eventHeader.put(SCHEMA_NAME, AVRO_SQL_TOPIC);
            return EventBuilder.withBody(eventBody, eventHeader);
        }
    }

    /**
     * The type Json.
     */
    public static class Json extends AbstractEntrySqlHandler {

        private static final Gson GSON = new Gson();
        private static final String AVRO_SQL_TOPIC = "json_ddl_sql";
        private static final Type TOKEN_TYPE = new TypeToken<Map<String, Object>>() {
        }.getType();

        /**
         * Instantiates a new Json.
         */
        public Json() {
            super(AVRO_SQL_TOPIC);
        }

        @Override
        Event dataToSqlEvent(Map<String, String> eventData, Map<String, String> eventHeader) {
            byte[] eventBody = GSON.toJson(eventData, TOKEN_TYPE)
                .getBytes(Charset.forName("UTF-8"));
            return EventBuilder.withBody(eventBody, eventHeader);
        }
    }

}


