package com.citic.source.canal;

import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER;
import static com.citic.sink.canal.KafkaSinkConstants.SCHEMA_HEADER;
import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_AGENT;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_DB;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_FROM;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_SQL;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TABLE;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TS;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.Utility;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

interface EntrySQLHandlerInterface {

    Event getSqlEvent(CanalEntry.Header entryHeader, String sql, CanalConf canalConf);
}

abstract class AbstractEntrySQLHandler implements EntrySQLHandlerInterface {

    private static final List<String> ATTR_LIST = Lists
        .newArrayList(META_FIELD_TABLE, META_FIELD_TS,
            META_FIELD_DB, META_FIELD_AGENT, META_FIELD_FROM, META_FIELD_SQL);
    private final String sql_topic_name;

    private AbstractEntrySQLHandler(String sql_topic_name) {
        this.sql_topic_name = sql_topic_name;
    }

    /*
     * 获取 sql topic Event数据
     * */
    public Event getSqlEvent(CanalEntry.Header entryHeader, String sql, CanalConf canalConf) {
        Map<String, String> eventSql = handleSQL(sql, entryHeader, canalConf);
        Map<String, String> sqlHeader = Maps.newHashMap();
        sqlHeader.put(DEFAULT_TOPIC_OVERRIDE_HEADER, sql_topic_name);
        return dataToSQLEvent(eventSql, sqlHeader);
    }

    abstract Event dataToSQLEvent(Map<String, String> eventData, Map<String, String> eventHeader);

    /*
     * 处理 sql topic 的数据格式
     * */
    private Map<String, String> handleSQL(String sql, CanalEntry.Header entryHeader,
        CanalConf canalConf) {
        Map<String, String> eventMap = Maps.newHashMap();
        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, DECIMAL_FORMAT_3.format(System.currentTimeMillis() / 1000.0));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_AGENT, canalConf.getAgentIPAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDBIP());
        eventMap.put(META_FIELD_SQL, Strings.isNullOrEmpty(sql) ? "no sql" : sql);
        return eventMap;
    }

    static class Avro extends AbstractEntrySQLHandler {

        private static final String AVRO_SQL_TOPIC = "avro_ddl_sql";

        Avro() {
            super(AVRO_SQL_TOPIC);
        }

        /*
         * 将 data, header 转换为 JSON Event 格式
         * */
        Event dataToSQLEvent(Map<String, String> eventData, Map<String, String> eventHeader) {
            String schemaString = Utility.getTableFieldSchema(ATTR_LIST, AVRO_SQL_TOPIC);
            byte[] eventBody = Utility.dataToAvroEventBody(eventData, ATTR_LIST, schemaString);
            // 用于sink解析
            eventHeader.put(SCHEMA_HEADER, schemaString);
            return EventBuilder.withBody(eventBody, eventHeader);
        }
    }

    static class Json extends AbstractEntrySQLHandler {

        private static final Gson GSON = new Gson();
        private static final String AVRO_SQL_TOPIC = "json_ddl_sql";
        private static Type TOKEN_TYPE = new TypeToken<Map<String, Object>>() {
        }.getType();

        Json() {
            super(AVRO_SQL_TOPIC);
        }

        @Override
        Event dataToSQLEvent(Map<String, String> eventData, Map<String, String> eventHeader) {
            byte[] eventBody = GSON.toJson(eventData, TOKEN_TYPE)
                .getBytes(Charset.forName("UTF-8"));
            return EventBuilder.withBody(eventBody, eventHeader);
        }
    }

}


