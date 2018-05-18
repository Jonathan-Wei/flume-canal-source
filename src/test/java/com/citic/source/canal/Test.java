package com.citic.source.canal;

import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;

import com.citic.helper.Utility;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
//        Runnable test =new Runnable() {
////            @Override
////            public void run() {
////                System.out.println(Thread.currentThread().getName() + "  "
////                    + Utility.Minutes5.getCurrentRounded5Minutes());
////            }
////        };
////
////        ExecutorService executors =  Executors.newFixedThreadPool(100);
////
////        for (int i = 0; i < 100; i++) {
////            executors.submit(test);
////        }
////        executors.shutdown();

//        Set<String> test = Sets.newLinkedHashSet();
//        test.add("name");
//        test.add("id");
//        test.add("age");
//        if (test.contains("name")) {
//            test.remove("name");
//            test.add("name");
//        }
//
//        test.forEach(item -> System.out.println(item));
        List<String> tst = Lists.newArrayList("id","name", "age");

        String test = Utility.getTableFieldSchema(tst, "zhoupeng");
        System.out.println(test);
    }

}

