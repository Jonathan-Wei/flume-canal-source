package com.citic.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
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

    private static final String COUNT_TOPIC = "count_topic";
    private static final String COUNT_PERIOD = "count_period";
    private static final String CURRENT_TIME = "current_time";
    private static final String COUNT = "count";

    private static final List<String> ATTR_LIST = Lists.newArrayList(COUNT_TOPIC,COUNT_PERIOD,
            CURRENT_TIME,COUNT);

    private static final Map<String, AtomicLong> CACHE_COUNTER = ExpiringMap.builder()
            .maxSize(10000)
            .expiration(2, TimeUnit.HOURS)
            .expirationPolicy(ExpirationPolicy.CREATED)
            .build();

    public static List<ProducerRecord> flowCounterToEvents(boolean useAvro) {
        List<ProducerRecord> records = Lists.newArrayList();
        CACHE_COUNTER.forEach((key, value) -> {
            if (useAvro)
                records.add(buildEachToEvent(key, value));
            else
                records.add(buildEachToJsonEvent(key, value));
        });
        return records;
    }

    private static ProducerRecord buildEachToEvent(String key, AtomicLong value) {
        String[] keyArray = key.split(":");

        String schemaString = Utility.getTableFieldSchema(ATTR_LIST, AVRO_FLOW_COUNTER_TOPIC);
        Schema schema = SchemaCache.getSchema(schemaString);
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put(COUNT_TOPIC, keyArray[0]);
        avroRecord.put(COUNT_PERIOD, keyArray[1]);
        avroRecord.put(CURRENT_TIME, new SimpleDateFormat(SUPPORT_TIME_FORMAT).format(new Date()));
        avroRecord.put(COUNT, value.toString());

        return new ProducerRecord<Object, Object>(AVRO_FLOW_COUNTER_TOPIC, avroRecord);
    }

    private static ProducerRecord buildEachToJsonEvent(String key, AtomicLong value) {
        String[] keyArray = key.split(":");
        byte[] eventBody;
        Map<String, String> eventData = Maps.newHashMap();
        eventData.put(COUNT_TOPIC, keyArray[0]);
        eventData.put(COUNT_PERIOD, keyArray[1]);
        eventData.put(CURRENT_TIME, new SimpleDateFormat(SUPPORT_TIME_FORMAT).format(new Date()));
        eventData.put(COUNT, value.toString());

        eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
        return new ProducerRecord<Object, Object>(JSON_FLOW_COUNTER_TOPIC, eventBody);
    }

    public static String getTimePeriodKey(String topic, String fieldName, Map<String, String> eventData) {
        if (fieldName == null)
            return null;

        String fieldValue = eventData.get(fieldName);
        if (fieldValue == null)
            return null;

        try {
            long timeStamp = Long.parseLong(fieldValue);
            return getTimePeriodKey(topic, timeStamp);
        } catch (NumberFormatException ex) {
            // 使用字符模式解析 时间戳
            return getTimePeriodKey(topic, fieldValue);
        }
    }

    private static String getTimePeriodKey(String topic, long timeStamp) {
        String timeKey = new SimpleDateFormat(TIME_KEY_FORMAT).format(new Date(timeStamp));
        return topic + ":" + timeKey;
    }


    private static String getTimePeriodKey(String topic, String timeStamp) {
        if (timeStamp == null)
            return null;
        DateTimeFormatter f = DateTimeFormat.forPattern(SUPPORT_TIME_FORMAT);

        try {
            DateTime dateTime = f.parseDateTime(timeStamp);
            String timeKey = new SimpleDateFormat(TIME_KEY_FORMAT).format(dateTime.toDate());

            return topic + ":" + timeKey;
        } catch (IllegalArgumentException ex) {
            LOGGER.error(ex.getMessage(), ex);
            return null;
        }
    }

    public static long incrementByKey(String key) {
        if (!CACHE_COUNTER.containsKey(key)) {
            CACHE_COUNTER.put(key, new AtomicLong(0));
        }
        return CACHE_COUNTER.get(key).incrementAndGet();
    }

}
