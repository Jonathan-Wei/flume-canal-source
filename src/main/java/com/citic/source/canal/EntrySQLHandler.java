package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.helper.Utility;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.helpers.Util;

import java.util.List;
import java.util.Map;

import static com.citic.source.canal.CanalSourceConstants.*;

class EntrySQLHandler {
    /*
    * 获取 sql topic Event数据
    * */
    static Event getSqlEvent(CanalEntry.Header entryHeader, String sql, CanalConf canalConf) {
        Map<String, String> eventSql = handleSQL(sql, entryHeader, canalConf);
        Map<String, String> sqlHeader = Maps.newHashMap();
        sqlHeader.put(HEADER_TOPIC, SQL);
        return dataToAvroSQLEvent(eventSql, sqlHeader);
    }

    /*
    * 将 data, header 转换为 JSON Event 格式
    * */
    private static Event dataToAvroSQLEvent(Map<String, String> eventData, Map<String, String> eventHeader) {
        List<String> attrList = Lists.newArrayList(META_FIELD_TABLE, META_FIELD_TS, META_FIELD_DB,
                                                    META_FIELD_AGENT, META_FIELD_FROM, SQL);

        Schema.Parser parser = new Schema.Parser();

        String schemaString = Utility.getTableFieldSchema(attrList, SQL);
        Schema schema = parser.parse(schemaString);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        GenericRecord avroRecord = new GenericData.Record(schema);

        for (String fieldStr: attrList) {
            avroRecord.put(fieldStr, eventData.get(fieldStr));
        }

        byte[] eventBody = recordInjection.apply(avroRecord);

        // 用于sink解析
        eventHeader.put(HEADER_SCHEMA, schemaString);
        return EventBuilder.withBody(eventBody,eventHeader);
    }

    /*
    * 处理 sql topic 的数据格式
    * */
    private static Map<String, String> handleSQL(String sql, CanalEntry.Header entryHeader, CanalConf canalConf) {
        Map<String, String > eventMap = Maps.newHashMap();
        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, String.valueOf(Math.round(entryHeader.getExecuteTime() / 1000)));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_AGENT, canalConf.getIPAddress());
        eventMap.put(META_FIELD_FROM, canalConf.getFromDBIP());
        eventMap.put(SQL, Strings.isNullOrEmpty(sql) ? "no sql" : sql );
        return eventMap;
    }
}
