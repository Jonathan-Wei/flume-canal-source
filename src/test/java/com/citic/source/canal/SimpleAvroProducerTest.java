package com.citic.source.canal;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

/**
 * Created by zhoupeng on 2018/4/9.
 */
public class SimpleAvroProducerTest {

    private static void main1() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.25:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getUserSchema());
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 1000; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "Str 1-" + i);
            avroRecord.put("str2", "Str 2-" + i);
            avroRecord.put("str3", "");

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("mytopic", bytes);
            producer.send(record);

            Thread.sleep(250);

        }

        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
//        System.out.printf(getUserSchema());
        main1();
    }

    private static String getUserSchema() {
        List<String> testList = Lists.newArrayList();
        List<String> resultList = Lists.newArrayList();
        testList.add("str1");
        testList.add("str2");
        testList.add("str3");
        String schema = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":[";

        for (String str: testList) {
            String field = "{ \"name\":\"" + str + "\", \"type\":\"string\" }";
            resultList.add(field);
        }
        schema += Joiner.on(",").join(resultList);
        schema += "]}";
        return schema;
    }

}
