package com.citic.helper;

import static com.citic.source.canal.CanalSourceConstants.GSON;
import static com.citic.source.canal.CanalSourceConstants.TOKEN_TYPE;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.nio.charset.Charset;
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


public class AgentCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AgentCounter.class);

    private static final String AVRO_AGENT_COUNTER_TOPIC = "avro_agent_counter";
    private static final String JSON_AGENT_COUNTER_TOPIC = "json_agent_counter";

    private static final String COUNT_AGENT = "agent";
    private static final String COUNT_PERIOD = "period";
    private static final String COUNT = "count";

    private static final List<String> ATTR_LIST = Lists
        .newArrayList(COUNT_AGENT, COUNT_PERIOD, COUNT);


    private static final Map<CounterKey, AtomicLong> CACHE_COUNTER = ExpiringMap.builder()
        .maxSize(10000)
        .expiration(10, TimeUnit.MINUTES)
        .expirationPolicy(ExpirationPolicy.CREATED)
        .build();


    public static List<ProducerRecord> flowCounterToEvents(boolean useAvro) {
        List<ProducerRecord> records = Lists.newArrayList();
        LOGGER.debug("AgentCounter CACHE_COUNTER: {}", CACHE_COUNTER);

        CACHE_COUNTER.forEach((key, value) -> {
            if (useAvro) {
                records.add(buildEachToEvent(key, value));
            } else {
                records.add(buildEachToJsonEvent(key, value));
            }
        });
        return records;
    }


    private static ProducerRecord buildEachToEvent(CounterKey key, AtomicLong value) {
        String schemaString = Utility.getTableFieldSchema(ATTR_LIST, AVRO_AGENT_COUNTER_TOPIC);
        Schema schema = SchemaCache.getSchema(schemaString);
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put(COUNT_AGENT, key.AgentIp);
        avroRecord.put(COUNT_PERIOD, key.minuteKey);
        avroRecord.put(COUNT, value.toString());

        return new ProducerRecord<Object, Object>(AVRO_AGENT_COUNTER_TOPIC, avroRecord);
    }

    private static ProducerRecord buildEachToJsonEvent(CounterKey key, AtomicLong value) {
        byte[] eventBody;
        Map<String, String> eventData = Maps.newHashMap();

        eventData.put(COUNT_AGENT, key.AgentIp);
        eventData.put(COUNT_PERIOD, key.minuteKey);
        eventData.put(COUNT, value.toString());

        eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
        return new ProducerRecord<Object, Object>(JSON_AGENT_COUNTER_TOPIC, eventBody);
    }

    public static void increment(String agentIp) {
        String minuteKey = Utility.Minutes5.getCurrentRounded5Minutes();
        CounterKey counterKey = new CounterKey(agentIp, minuteKey);
        incrementByKey(counterKey);
    }

    private static long incrementByKey(CounterKey key) {
        if (!CACHE_COUNTER.containsKey(key)) {
            CACHE_COUNTER.put(key, new AtomicLong(0));
        }
        return CACHE_COUNTER.get(key).incrementAndGet();
    }

    private static class CounterKey {

        private final String AgentIp;
        private final String minuteKey;

        CounterKey(String agentIp, String minuteKey) {
            AgentIp = agentIp;
            this.minuteKey = minuteKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CounterKey that = (CounterKey) o;

            return AgentIp.equals(that.AgentIp) && minuteKey.equals(that.minuteKey);

        }

        @Override
        public int hashCode() {
            int result = AgentIp.hashCode();
            result = 31 * result + minuteKey.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "CounterKey{" +
                "AgentIp='" + AgentIp + '\'' +
                ", minuteKey='" + minuteKey + '\'' +
                '}';
        }
    }
}
