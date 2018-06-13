package com.citic.source.canal;

import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;

import com.citic.helper.RegexHashMap;
import com.citic.helper.Utility;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
        Map<String, String> tableToTopicRegexMap = new RegexHashMap<>();

        tableToTopicRegexMap.put(".*\\..*", "test");

        System.out.println(tableToTopicRegexMap.get(".*\\..*"));

    }

}

