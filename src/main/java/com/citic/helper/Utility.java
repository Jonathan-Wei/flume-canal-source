package com.citic.helper;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Utility {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utility.class);
    private static final String DEFAULT_IP = "127.0.0.1";

    public static String getLocalIP(String interfaceName) {
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
                    if (ia instanceof Inet6Address)
                        continue;
                    ip = ia.getHostAddress();
                }
                break;
            }
        }
        return ip;
    }


    /*
    * get schema String
    * */
    public static String getTableFieldSchema(List<String> schemaFieldList, String schemaName) {
        StringBuilder builder = new StringBuilder();
        String schema = "{"
                + "\"type\":\"record\","
                + "\"name\":\""+ schemaName +"\","
                + "\"fields\":[";

        builder.append(schema);
        String prefix = "";
        for (String fieldStr: schemaFieldList) {
            String field = "{ \"name\":\"" + fieldStr + "\", \"type\":\"string\" }";
            builder.append(prefix);
            prefix = ",";
            builder.append(field);
        }
        builder.append("]}");
        return builder.toString();
    }

    /*
    * avro Record to json string
    * */
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
            return new String(output.toByteArray());
        } catch (IOException e) {
            LOGGER.error("avroToJson error, avroRecord: {}", avroRecord, e);
            return "invalid avro record";
        } finally {
            try { if (output != null) output.close(); } catch (Exception ignored) { }
        }
    }

    public static byte[] dataToAvroEventBody(Map<String, String> eventData,
                                             List<String> fieldList,
                                             String schemaString) {
        Schema schema = SchemaCache.getSchema(schemaString);
        GenericRecord avroRecord = new GenericData.Record(schema);

        for (String fieldStr: fieldList) {
            avroRecord.put(fieldStr, eventData.getOrDefault(fieldStr, ""));
        }

        return AvroRecordSerDe.serialize(avroRecord, schema);
    }

    static class Minutes5 {
        private static final String TIME_MINUTE_FORMAT = "yyyy-MM-dd HH:mm";
        private static final Calendar calendar = GregorianCalendar.getInstance();
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

        static String getCurrentRounded5Minutes() {
            calendar.setTime(new Date());
            int unroundedMinutes = calendar.get(Calendar.MINUTE);
            calendar.set(Calendar.MINUTE, unroundedMinutes - (unroundedMinutes % 5));
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            return formatCache.getUnchecked(calendar.getTime());
        }
    }
}
