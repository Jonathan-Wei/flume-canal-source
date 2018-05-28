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


/**
 * The type Agent counter.
 */
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


    /**
     * Flow counter to events list.
     *
     * @param useAvro the use avro
     * @return the list
     */
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

        Schema schema = SchemaCache.getSchema(ATTR_LIST, AVRO_AGENT_COUNTER_TOPIC);
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put(COUNT_AGENT, key.agentIp);
        avroRecord.put(COUNT_PERIOD, key.minuteKey);
        avroRecord.put(COUNT, value.toString());

        return new ProducerRecord<Object, Object>(AVRO_AGENT_COUNTER_TOPIC, key.minuteKey,
            avroRecord);
    }

    private static ProducerRecord buildEachToJsonEvent(CounterKey key, AtomicLong value) {
        Map<String, String> eventData = Maps.newHashMap();

        eventData.put(COUNT_AGENT, key.agentIp);
        eventData.put(COUNT_PERIOD, key.minuteKey);
        eventData.put(COUNT, value.toString());

        byte[] eventBody;
        eventBody = GSON.toJson(eventData, TOKEN_TYPE).getBytes(Charset.forName("UTF-8"));
        return new ProducerRecord<Object, Object>(JSON_AGENT_COUNTER_TOPIC, key.minuteKey,
            eventBody);
    }

    /**
     * Increment.
     *
     * @param agentIp the agent ip
     */
    public static void increment(String agentIp) {
        String minuteKey = Utility.Minutes5.getCurrentRounded5Minutes();
        CounterKey counterKey = new CounterKey(agentIp, minuteKey);
        incrementByKey(counterKey);
    }

    private static long incrementByKey(CounterKey key) {
        return CACHE_COUNTER.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    private static class CounterKey {

        private final String agentIp;
        private final String minuteKey;

        /**
         * Instantiates a new Counter key.
         *
         * @param agentIp the agent ip
         * @param minuteKey the minute key
         */
        CounterKey(String agentIp, String minuteKey) {
            this.agentIp = agentIp;
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

            return agentIp.equals(that.agentIp) && minuteKey.equals(that.minuteKey);

        }

        @Override
        public int hashCode() {
            int result = agentIp.hashCode();
            result = 31 * result + minuteKey.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "CounterKey{"
                + "agentIp='" + agentIp + '\''
                + ", minuteKey='" + minuteKey + '\''
                + '}';
        }
    }
}
