package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.Utility;
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
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.helpers.Util;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static com.citic.source.canal.CanalSourceConstants.*;

abstract class EntrySQLHandler {
    private static final List<String> ATTR_LIST = Lists.newArrayList(META_FIELD_TABLE, META_FIELD_TS, META_FIELD_DB,
            META_FIELD_AGENT, META_FIELD_FROM, SQL);
    /*
    * 获取 sql topic Event数据
    * */
    Event getSqlEvent(CanalEntry.Header entryHeader, String sql, CanalConf canalConf) {
        Map<String, String> eventSql = handleSQL(sql, entryHeader, canalConf);
        Map<String, String> sqlHeader = Maps.newHashMap();
        sqlHeader.put(HEADER_TOPIC, SQL);
        return dataToSQLEvent(eventSql, sqlHeader);
    }

    abstract Event dataToSQLEvent(Map<String, String> eventData, Map<String, String> eventHeader);

    /*
    * 处理 sql topic 的数据格式
    * */
    private Map<String, String> handleSQL(String sql, CanalEntry.Header entryHeader, CanalConf canalConf) {
        Map<String, String > eventMap = Maps.newHashMap();
        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, String.valueOf(Math.round(entryHeader.getExecuteTime() / 1000)));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_AGENT, canalConf.getIPAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDBIP());
        eventMap.put(SQL, Strings.isNullOrEmpty(sql) ? "no sql" : sql );
        return eventMap;
    }

    static class Avro extends EntrySQLHandler {
        /*
    * 将 data, header 转换为 JSON Event 格式
    * */
        Event dataToSQLEvent(Map<String, String> eventData, Map<String, String> eventHeader) {

            Schema.Parser parser = new Schema.Parser();

            String schemaString = Utility.getTableFieldSchema(ATTR_LIST, SQL);
            Schema schema = parser.parse(schemaString);
            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

            GenericRecord avroRecord = new GenericData.Record(schema);

            for (String fieldStr: ATTR_LIST) {
                avroRecord.put(fieldStr, eventData.get(fieldStr));
            }

            byte[] eventBody = recordInjection.apply(avroRecord);

            // 用于sink解析
            eventHeader.put(HEADER_SCHEMA, schemaString);
            return EventBuilder.withBody(eventBody,eventHeader);
        }
    }

    static class Json extends EntrySQLHandler {
        private static final Gson GSON = new Gson();
        private static Type TOKEN_TYPE = new TypeToken<Map<String, Object>>(){}.getType();

        @Override
        Event dataToSQLEvent(Map<String, String> eventData, Map<String, String> eventHeader) {
            byte[] eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
            return EventBuilder.withBody(eventBody,eventHeader);
        }
    }

}


