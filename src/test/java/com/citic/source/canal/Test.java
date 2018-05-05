package com.citic.source.canal;

import com.citic.helper.AvroRecordSerDe;
import com.citic.helper.SchemaCache;
import com.citic.helper.Utility;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {

        String result = String.valueOf(System.currentTimeMillis() / 1000.0);
        System.out.println(String.format("%.3f", System.currentTimeMillis() / 1000.0));

        List<String> test = Lists.newArrayList("id", "name", "age");
        String testSchema = "test";

        DecimalFormat df2 = new DecimalFormat(".###");

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            String.format("%.3f", System.currentTimeMillis() / 1000.0);
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);

        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            DECIMAL_FORMAT_3.format(System.currentTimeMillis() / 1000.0);
        }
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);

        System.out.println(DECIMAL_FORMAT_3.format(System.currentTimeMillis() / 1000.0));
    }

}

