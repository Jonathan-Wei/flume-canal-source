package com.citic.source.canal;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.avro.Schema;
import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
        Calendar calendar = GregorianCalendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");


        LoadingCache<Date, String> formatCache = CacheBuilder
                .newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(6,TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Date, String>() {
                            @Override
                            public String load(Date date) {

                                System.out.printf("%s format ..", date.toString());
                                return dateFormat.format(date);
                            }
                        });


        while (true) {
            TimeUnit.SECONDS.sleep(10);


            calendar.setTime(new Date());
            int unroundedMinutes = calendar.get(Calendar.MINUTE);
            calendar.set(Calendar.MINUTE, unroundedMinutes - (unroundedMinutes % 5));
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            System.out.println(formatCache.getUnchecked(calendar.getTime()));

//            long timeMs = System.currentTimeMillis();
//            long roundedtimeMs = Math.round( (double)timeMs/(5*60*1000) ) * (5*60*1000);

//            long timeMs = System.currentTimeMillis();
//            long minute5 = 5*60*1000;
//
//            long roundedtimeMs = Math.round((double)timeMs/(double)(minute5)) * (minute5);

//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//            Date date = new Date(roundedtimeMs);
//            System.out.println(dateFormat.format(date));
        }


    }

}

