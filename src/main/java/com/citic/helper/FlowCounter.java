package com.citic.helper;

import static com.citic.source.canal.CanalSourceConstants.GSON;
import static com.citic.source.canal.CanalSourceConstants.TOKEN_TYPE;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The type Flow counter.
 */
public class FlowCounter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlowCounter.class);
    private static final String TIME_KEY_FORMAT = "yyyy-MM-dd HH";
    private static final String SUPPORT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String AVRO_FLOW_COUNTER_TOPIC = "avro_flow_counter";
    private static final String JSON_FLOW_COUNTER_TOPIC = "json_flow_counter";
    private static final String AVRO_FLOW_ERROR_COUNTER_TOPIC = "avro_flow_error_counter";
    private static final String JSON_FLOW_ERROR_COUNTER_TOPIC = "json_flow_error_counter";

    private static final String COUNT_TOPIC = "topic";
    private static final String COUNT_TABLE = "table";
    private static final String COUNT_FROM = "from";
    private static final String COUNT_PERIOD = "period";
    private static final String CURRENT_TIME = "ctime";
    private static final String COUNT = "count";

    private static final List<String> ATTR_LIST = Lists
        .newArrayList(COUNT_TOPIC, COUNT_TABLE, COUNT_FROM,
            COUNT_PERIOD, CURRENT_TIME, COUNT);

    private static final Map<FlowCounterKey, AtomicLong> CACHE_COUNTER = ExpiringMap.builder()
        .maxSize(10000)
        .expiration(2, TimeUnit.HOURS)
        .expirationPolicy(ExpirationPolicy.CREATED)
        .build();

    private static final Map<FlowCounterKey, AtomicLong> ERROR_CACHE_COUNTER = ExpiringMap.builder()
        .maxSize(10000)
        .expiration(2, TimeUnit.HOURS)
        .expirationPolicy(ExpirationPolicy.CREATED)
        .build();

    /**
     * Flow counter to events list.
     *
     * @param useAvro the use avro
     * @return the list
     */
    public static List<ProducerRecord> flowCounterToEvents(boolean useAvro) {
        List<ProducerRecord> records = Lists.newArrayList();
        LOGGER.debug("FlowCounter CACHE_COUNTER: {}", CACHE_COUNTER);
        LOGGER.debug("FlowCounter ERROR_CACHE_COUNTER: {}", ERROR_CACHE_COUNTER);

        CACHE_COUNTER.forEach((key, value) -> {
            if (useAvro) {
                records.add(buildEachToEvent(key, value, AVRO_FLOW_COUNTER_TOPIC));
            } else {
                records.add(buildEachToJsonEvent(key, value, JSON_FLOW_COUNTER_TOPIC));
            }
        });

        ERROR_CACHE_COUNTER.forEach((key, value) -> {
            if (useAvro) {
                records.add(buildEachToEvent(key, value, AVRO_FLOW_ERROR_COUNTER_TOPIC));
            } else {
                records.add(buildEachToJsonEvent(key, value, JSON_FLOW_ERROR_COUNTER_TOPIC));
            }
        });
        return records;
    }

    private static ProducerRecord buildEachToEvent(FlowCounterKey key, AtomicLong value,
        String topic) {
        Schema schema = SchemaCache.getSchema(ATTR_LIST, topic);
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put(COUNT_TOPIC, key.topic);
        avroRecord.put(COUNT_TABLE, key.table);
        avroRecord.put(COUNT_FROM, key.fromDb);
        avroRecord.put(COUNT_PERIOD, key.timePeriod);
        avroRecord.put(CURRENT_TIME, new SimpleDateFormat(SUPPORT_TIME_FORMAT).format(new Date()));
        avroRecord.put(COUNT, value.toString());

        return new ProducerRecord<Object, Object>(topic, key.timePeriod,
            avroRecord);
    }

    private static ProducerRecord buildEachToJsonEvent(FlowCounterKey key, AtomicLong value,
        String topic) {
        Map<String, String> eventData = Maps.newHashMap();

        eventData.put(COUNT_TOPIC, key.topic);
        eventData.put(COUNT_TABLE, key.table);
        eventData.put(COUNT_FROM, key.fromDb);
        eventData.put(COUNT_PERIOD, key.timePeriod);
        eventData.put(CURRENT_TIME, new SimpleDateFormat(SUPPORT_TIME_FORMAT).format(new Date()));
        eventData.put(COUNT, value.toString());

        byte[] eventBody;
        eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
        return new ProducerRecord<Object, Object>(topic, key.timePeriod,
            eventBody);
    }


    /**
     * Increment error by key long.
     *
     * @param key the key
     * @return the long
     */
    public static long incrementErrorByKey(FlowCounterKey key) {
        LOGGER.debug("FlowErrorCounter, key: {}", key.toString());
        return ERROR_CACHE_COUNTER.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    /**
     * Increment.
     *
     * @param topic the topic
     * @param table the table
     * @param fromDb the from db
     * @param fieldValue the field value
     * @return the flow counter key
     */
    public static FlowCounterKey increment(String topic, String table, String fromDb,
        String fieldValue) {
        String timePeriod = getTimePeriodKey(fieldValue);
        FlowCounterKey totalKey = null;
        if (timePeriod != null) {
            totalKey = new FlowCounterKey(topic, table, fromDb, timePeriod);
            incrementByKey(totalKey);
        }
        return totalKey;
    }

    private static String getTimePeriodKey(String timeStamp) {
        if (timeStamp == null) {
            return null;
        }

        if (timeStamp.length() >= SUPPORT_TIME_FORMAT.length()) {
            return timeStamp.substring(0, TIME_KEY_FORMAT.length());
        } else {
            return null;
        }
    }

    private static long incrementByKey(FlowCounterKey key) {
        LOGGER.debug("FlowCounter, key: {}", key.toString());
        return CACHE_COUNTER.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    /**
     * The type Flow counter key.
     */
    public static class FlowCounterKey {

        private final String topic;
        private final String table;
        private final String fromDb;
        private final String timePeriod;

        /**
         * Instantiates a new Flow counter key.
         *
         * @param topic the topic
         * @param table the table
         * @param fromDb the from db
         * @param timePeriod the time period
         */
        public FlowCounterKey(String topic, String table, String fromDb, String timePeriod) {
            this.topic = topic;
            this.table = table;
            this.fromDb = fromDb;
            this.timePeriod = timePeriod;
        }

        /**
         * Gets topic.
         *
         * @return the topic
         */
        public String getTopic() {
            return topic;
        }

        /**
         * Gets table.
         *
         * @return the table
         */
        public String getTable() {
            return table;
        }

        /**
         * Gets from db.
         *
         * @return the from db
         */
        public String getFromDb() {
            return fromDb;
        }

        /**
         * Gets time period.
         *
         * @return the time period
         */
        public String getTimePeriod() {
            return timePeriod;
        }

        @Override
        public String toString() {
            return "FlowCounterKey{"
                + "topic='" + topic + '\''
                + ", table='" + table + '\''
                + ", fromDb='" + fromDb + '\''
                + ", timePeriod='" + timePeriod + '\''
                + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FlowCounterKey that = (FlowCounterKey) o;

            return topic.equals(that.topic)
                && table.equals(that.table)
                && fromDb.equals(that.fromDb)
                && timePeriod.equals(that.timePeriod);

        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + table.hashCode();
            result = 31 * result + fromDb.hashCode();
            result = 31 * result + timePeriod.hashCode();
            return result;
        }
    }

}
