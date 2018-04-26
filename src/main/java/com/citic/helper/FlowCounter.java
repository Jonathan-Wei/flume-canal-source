package com.citic.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.citic.source.canal.CanalSourceConstants.GSON;
import static com.citic.source.canal.CanalSourceConstants.TOKEN_TYPE;


public class FlowCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowCounter.class);
    private static final String TIME_KEY_FORMAT = "yyyy-MM-dd HH";
    private static final String SUPPORT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String AVRO_FLOW_COUNTER_TOPIC = "avro_flow_counter";
    private static final String JSON_FLOW_COUNTER_TOPIC = "json_flow_counter";

    private static final String COUNT_TOPIC = "topic";
    private static final String COUNT_TABLE = "table";
    private static final String COUNT_FROM = "from";
    private static final String COUNT_PERIOD = "period";
    private static final String CURRENT_TIME = "ctime";
    private static final String COUNT = "count";

    private static final List<String> ATTR_LIST = Lists.newArrayList(COUNT_TOPIC,COUNT_TABLE,COUNT_FROM,
            COUNT_PERIOD, CURRENT_TIME,COUNT);

    private static final Map<CounterKey, AtomicLong> CACHE_COUNTER = ExpiringMap.builder()
            .maxSize(10000)
            .expiration(2, TimeUnit.HOURS)
            .expirationPolicy(ExpirationPolicy.CREATED)
            .build();

    public static List<ProducerRecord> flowCounterToEvents(boolean useAvro) {
        List<ProducerRecord> records = Lists.newArrayList();
        LOGGER.debug("FlowCounter CACHE_COUNTER: {}", CACHE_COUNTER);

        CACHE_COUNTER.forEach((key, value) -> {
            if (useAvro)
                records.add(buildEachToEvent(key, value));
            else
                records.add(buildEachToJsonEvent(key, value));
        });
        return records;
    }

    private static ProducerRecord buildEachToEvent(CounterKey key, AtomicLong value) {

        String schemaString = Utility.getTableFieldSchema(ATTR_LIST, AVRO_FLOW_COUNTER_TOPIC);
        Schema schema = SchemaCache.getSchema(schemaString);
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put(COUNT_TOPIC, key.topic);
        avroRecord.put(COUNT_TABLE, key.table);
        avroRecord.put(COUNT_FROM, key.fromDB);
        avroRecord.put(COUNT_PERIOD, key.timePeriod);
        avroRecord.put(CURRENT_TIME, new SimpleDateFormat(SUPPORT_TIME_FORMAT).format(new Date()));
        avroRecord.put(COUNT, value.toString());

        return new ProducerRecord<Object, Object>(AVRO_FLOW_COUNTER_TOPIC, avroRecord);
    }

    private static ProducerRecord buildEachToJsonEvent(CounterKey key, AtomicLong value) {
        byte[] eventBody;
        Map<String, String> eventData = Maps.newHashMap();

        eventData.put(COUNT_TOPIC, key.topic);
        eventData.put(COUNT_TABLE, key.table);
        eventData.put(COUNT_FROM, key.fromDB);
        eventData.put(COUNT_PERIOD, key.timePeriod);
        eventData.put(CURRENT_TIME, new SimpleDateFormat(SUPPORT_TIME_FORMAT).format(new Date()));
        eventData.put(COUNT, value.toString());

        eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
        return new ProducerRecord<Object, Object>(JSON_FLOW_COUNTER_TOPIC, eventBody);
    }


    public static void increment(String topic, String table, String fromDB,
                                   String fieldValue) {
        String timePeriod = getTimePeriodKey(fieldValue);
        if (timePeriod != null) {
            CounterKey totalKey  = new CounterKey(topic, table, fromDB, timePeriod);
            incrementByKey(totalKey);
        }
    }

    private static String getTimePeriodKey(String timeStamp) {
        if (timeStamp == null)
            return null;
        // 只支持 数据库中的 datetime和 timestamp 格式
        if (timeStamp.length() != SUPPORT_TIME_FORMAT.length())
            return null;
        return timeStamp.substring(0, TIME_KEY_FORMAT.length());
    }

    private static long incrementByKey(CounterKey key) {
        if (!CACHE_COUNTER.containsKey(key)) {
            CACHE_COUNTER.put(key, new AtomicLong(0));
        }
        return CACHE_COUNTER.get(key).incrementAndGet();
    }

    private static class CounterKey {
        private final String topic;
        private final String table;
        private final String fromDB;
        private final String timePeriod;

        private CounterKey(String topic, String table, String fromDB, String timePeriod) {
            this.topic = topic;
            this.table = table;
            this.fromDB = fromDB;
            this.timePeriod = timePeriod;
        }

        @Override
        public String toString() {
            return "CounterKey{" +
                    "topic='" + topic + '\'' +
                    ", table='" + table + '\'' +
                    ", fromDB='" + fromDB + '\'' +
                    ", timePeriod='" + timePeriod + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CounterKey that = (CounterKey) o;

            return topic.equals(that.topic)
                    && table.equals(that.table)
                    && fromDB.equals(that.fromDB)
                    && timePeriod.equals(that.timePeriod);

        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + table.hashCode();
            result = 31 * result + fromDB.hashCode();
            result = 31 * result + timePeriod.hashCode();
            return result;
        }
    }

}
