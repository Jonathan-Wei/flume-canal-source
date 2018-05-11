package com.citic.source.canal;

import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;

import com.google.common.collect.Lists;
import java.text.DecimalFormat;
import java.util.List;

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

