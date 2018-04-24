package com.citic.source.canal;

import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            TimeUnit.SECONDS.sleep(10);

            Calendar calendar = Calendar.getInstance();
            int unroundedMinutes = calendar.get(Calendar.MINUTE);
            calendar.add(Calendar.MINUTE, - (unroundedMinutes % 5));

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//            dateFormat.setTimeZone(calendar.getTimeZone());
            System.out.println(dateFormat.format(calendar.getTime()));

        }


    }

}

