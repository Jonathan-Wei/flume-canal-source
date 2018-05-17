package com.citic.helper;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
     * Gets table field schema 2.
     *
     * @param schemaFieldList the schema field list
     * @param attrList the attr list
     * @param schemaName the schema name
     * @return the table field schema 2
     */
    public static String getTableFieldSchema2(Iterable<String> schemaFieldList,
        Iterable<String> attrList, String schemaName) {
        StringBuilder builder = new StringBuilder();
        String schema = "{"
            + "\"type\":\"record\","
            + "\"name\":\"" + schemaName + "\","
            + "\"fields\":[";

        builder.append(schema);
        String prefix = "";
        for (String fieldStr : schemaFieldList) {
            String field = "{ \"name\":\"" + fieldStr + "\", \"type\":\"string\" }";
            builder.append(prefix);
            prefix = ",";
            builder.append(field);
        }

        for (String fieldStr : attrList) {
            String field = "{ \"name\":\"" + fieldStr + "\", \"type\":\"string\" }";
            builder.append(prefix);
            prefix = ",";
            builder.append(field);
        }

        builder.append("]}");
        return builder.toString();
    }


    /**
     * Gets table field schema.
     *
     * @param schemaFieldList the schema field list
     * @param schemaName the schema name
     * @return the table field schema
     */
    public static String getTableFieldSchema(Iterable<String> schemaFieldList, String schemaName) {
        StringBuilder builder = new StringBuilder();
        String schema = "{"
            + "\"type\":\"record\","
            + "\"name\":\"" + schemaName + "\","
            + "\"fields\":[";

        builder.append(schema);
        String prefix = "";
        for (String fieldStr : schemaFieldList) {
            String field = "{ \"name\":\"" + fieldStr + "\", \"type\":\"string\" }";
            builder.append(prefix);
            prefix = ",";
            builder.append(field);
        }

        builder.append("]}");
        return builder.toString();
    }


    /**
     * Avro to json string.
     *
     * @param avroRecord the avro record
     * @return the string
     */
    public static String avroToJson(GenericRecord avroRecord) {
        JsonEncoder encoder;
        ByteArrayOutputStream output = null;
        try {
            output = new ByteArrayOutputStream();
            Schema schema = avroRecord.getSchema();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            encoder = EncoderFactory.get().jsonEncoder(schema, output);
            writer.write(avroRecord, encoder);
            encoder.flush();
            output.flush();
            return new String(output.toByteArray(), UTF_8);
        } catch (IOException e) {
            LOGGER.error("avroToJson error, avroRecord: {}", avroRecord, e);
            return "invalid avro record";
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Data to avro event body byte [ ].
     *
     * @param eventData the event data
     * @param fieldList the field list
     * @param schemaString the schema string
     * @return the byte [ ]
     */
    public static byte[] dataToAvroEventBody(Map<String, String> eventData,
        List<String> fieldList,
        String schemaString) {
        Schema schema = SchemaCache.getSchema(schemaString);
        GenericRecord avroRecord = new GenericData.Record(schema);

        for (String fieldStr : fieldList) {
            avroRecord.put(fieldStr, eventData.getOrDefault(fieldStr, ""));
        }

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        return recordInjection.apply(avroRecord);
    }

    /**
     * The type Minutes 5.
     */
    public static class Minutes5 {

        private static final String TIME_MINUTE_FORMAT = "yyyy-MM-dd HH:mm";
        private static final ThreadLocal<Calendar> threadLocalCanendar = new ThreadLocal<>();
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_MINUTE_FORMAT);

        private static final LoadingCache<Date, String> formatCache = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(6, TimeUnit.MINUTES)
            .build(new CacheLoader<Date, String>() {
                @Override
                public String load(Date date) {
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
