package com.citic.helper;

import static com.citic.sink.canal.KafkaSinkConstants.AGENT_COUNTER_AGENT_IP;
import static com.citic.sink.canal.KafkaSinkConstants.AGENT_COUNTER_MINUTE_KEY;
import static com.citic.sink.canal.KafkaSinkConstants.FLOW_COUNTER_FROM_DB;
import static com.citic.sink.canal.KafkaSinkConstants.FLOW_COUNTER_TABLE;
import static com.citic.sink.canal.KafkaSinkConstants.FLOW_COUNTER_TIME_PERIOD;
import static com.citic.sink.canal.KafkaSinkConstants.FLOW_COUNTER_TOPIC;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.citic.helper.AgentCounter.AgentCounterKey;
import com.citic.helper.FlowCounter.FlowCounterKey;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Utility.
 */
public class Utility {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utility.class);
    private static final String DEFAULT_IP = "127.0.0.1";

    private Utility() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Gets local ip.
     *
     * @param interfaceName the interface name
     * @return the local ip
     */
    public static String getLocalIp(String interfaceName) {
        String ip = DEFAULT_IP;
        Enumeration<?> e1;
        try {
            e1 = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            LOGGER.error(e.getMessage(), e);
            return ip;
        }

        while (e1.hasMoreElements()) {
            NetworkInterface ni = (NetworkInterface) e1.nextElement();
            if (ni.getName().equals(interfaceName)) {
                Enumeration<?> e2 = ni.getInetAddresses();
                while (e2.hasMoreElements()) {
                    InetAddress ia = (InetAddress) e2.nextElement();
                    if (ia instanceof Inet6Address) {
                        continue;
                    }
                    ip = ia.getHostAddress();
                }
                break;
            }
        }
        return ip;
    }

    /**
     * Avro to json string.
     *
     * @param avroRecord the avro record
     * @return the string
     */
    public static String avroToJson(GenericRecord avroRecord) {
        JsonEncoder encoder;
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            Schema schema = avroRecord.getSchema();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            encoder = EncoderFactory.get().jsonEncoder(schema, output);
            writer.write(avroRecord, encoder);
            encoder.flush();
            output.flush();
            return new String(output.toByteArray(), UTF_8);
        } catch (IOException e) {
            LOGGER.error("avroToJson error, avroRecord: {}", avroRecord, e);
            return "invalid avro record";
        }
    }


    /**
     * Put flow counter key to header.
     *
     * @param header the header
     * @param counterKey the counter key
     */
    public static void putFlowCounterKeyToHeader(Map<String, String> header,
        FlowCounterKey counterKey) {
        if (counterKey != null) {
            header.put(FLOW_COUNTER_TOPIC, counterKey.getTopic());
            header.put(FLOW_COUNTER_TABLE, counterKey.getTable());
            header.put(FLOW_COUNTER_FROM_DB, counterKey.getFromDb());
            header.put(FLOW_COUNTER_TIME_PERIOD, counterKey.getTimePeriod());
        }
    }

    /**
     * Put agent counter key to header.
     *
     * @param header the header
     * @param counterKey the counter key
     */
    public static void putAgentCounterKeyToHeader(Map<String, String> header,
        AgentCounterKey counterKey) {
        if (counterKey != null) {
            header.put(AGENT_COUNTER_AGENT_IP, counterKey.getAgentIp());
            header.put(AGENT_COUNTER_MINUTE_KEY, counterKey.getMinuteKey());
        }
    }

    /**
     * The type Minutes 5.
     */
    public static class Minutes5 {

        private static final String TIME_MINUTE_FORMAT = "yyyy-MM-dd HH:mm";
        private static final ThreadLocal<Calendar> threadLocalCanendar = new ThreadLocal<>();

        private Minutes5() {
            throw new IllegalStateException("Utility class");
        }

        private static final LoadingCache<Date, String> formatCache = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(6, TimeUnit.MINUTES)
            .build(new CacheLoader<Date, String>() {
                @Override
                public String load(Date date) {
                    SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_MINUTE_FORMAT);
                    return dateFormat.format(date);
                }
            });

        /**
         * Gets current rounded 5 minutes.
         *
         * @return the current rounded 5 minutes
         */
        public static String getCurrentRounded5Minutes() {
            Calendar calendar = threadLocalCanendar.get();
            if (calendar == null) {
                calendar = GregorianCalendar.getInstance();
                threadLocalCanendar.set(calendar);
            }
            calendar.setTime(new Date());
            int unroundedMinutes = calendar.get(Calendar.MINUTE);
            calendar.set(Calendar.MINUTE, unroundedMinutes - (unroundedMinutes % 5));
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            return formatCache.getUnchecked(calendar.getTime());
        }
    }
}
