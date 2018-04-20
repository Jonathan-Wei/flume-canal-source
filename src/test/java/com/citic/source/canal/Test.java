package com.citic.source.canal;

import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
//        MetricRegistry metricRegistry = new MetricRegistry();
//
//        Meter meter2 = metricRegistry.meter("meter2");
//
//        Gauge<Long> activeUsersGauge = new ActiveUsersGauge(5, TimeUnit.SECONDS);

//        for (int i = 0; i < 100; i++) {
//            TimeUnit.SECONDS.sleep(1);
//            Long value =  activeUsersGauge.getValue();
//            System.out.printf(String.valueOf(value) );
//
//        }


        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss")
//                .format(new Date(Long.parseLong("1524136724014")));
                .format(new Date(Long.parseLong("232323")));
        System.out.println(timeStamp);


//        LoadingCache<String, Counter> graphs = CacheBuilder.newBuilder()
//                .concurrencyLevel(4)
//                .weakKeys()
//                .maximumSize(10000)
//                .expireAfterWrite(10, TimeUnit.MINUTES)
//                .build(
//                        new CacheLoader<String, Counter>() {
//                            public Counter load(String key) {
//                                return new Counter();
//                            }
//                        });


//        Map<String, String> map = ExpiringMap.builder()
//                .maxSize(123)
//                .expiration(3, TimeUnit.SECONDS)
//                .expirationPolicy(ExpirationPolicy.ACCESSED)
//                .build();
//
//        map.put("hello", "HELLO");
//        map.put("wew", "HELLO");
//        map.put("qwqw", "HELLO");
//
//                for (int i = 0; i < 100; i++) {
//            TimeUnit.SECONDS.sleep(1);
//                    System.out.println(map.keySet().toString());
//                    System.out.println(map.get("hello"));
//        }


    }

}

