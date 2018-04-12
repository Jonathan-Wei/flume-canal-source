package com.citic.helper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

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
        List<String> resultList = Lists.newArrayList();
        String schema = "{"
                + "\"type\":\"record\","
                + "\"name\":\""+ schemaName +"\","
                + "\"fields\":[";

        for (String fieldStr: schemaFieldList) {
            String field = "{ \"name\":\"" + fieldStr + "\", \"type\":\"string\" }";
            resultList.add(field);
        }
        schema += Joiner.on(",").join(resultList);
        schema += "]}";
        return schema;
    }

    /*
    * avro Record to json string
    * */
    public static String avroToJson(GenericRecord avroRecord) throws IOException {
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
        } finally {
            try { if (output != null) output.close(); } catch (Exception ignored) { }
        }
    }
}
