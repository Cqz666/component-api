package com.cqz.component.common.utils;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.util.Calendar;
import java.util.Date;

import static java.time.temporal.ChronoField.*;

public class Md5Test {
    public static void main(String[] args) throws IOException {
        String s = DigestUtils.md5Hex(new FileInputStream("/home/cqz/realtime-etl-1.0-SNAPSHOT.jar"));
        System.out.println(s);

        String s1 = DigestUtils.md5Hex("/sdjsad/dqweqweqw/dwqdqwdq/dqwdqw2").substring(8,24);

        String sss = DigestUtils.md5Hex(
                "/home/ftp/flink_stream/pro/workspaces/uhtyr3gp97/project_spaces/ms56biwocv4n8984/artifacts/").substring(8,24);
        System.out.println(sss+":ac7bd43aa686ae9de469226dcc921dc1");

        String str = "/hive/warehouse/default/gamebox_event_iceberg/data/datekey=20220521/event=like_comment";
        String substring = str.substring(str.lastIndexOf("/") + 1).replace("event=","");
        System.out.println(substring);
        System.out.println(Runtime.getRuntime().availableProcessors());

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
//        calendar.setTimeInMillis(1653634811000L);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        System.out.println("hour = "+hour);
        if (hour == 9){
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            String yyyyMMdd = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime());
            System.out.println(yyyyMMdd);
        }

        System.out.println(Runtime.getRuntime().availableProcessors());

        format(81810721745L+1577808000000L);


    }
    private static final DateTimeFormatter DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.LENIENT);

    public static void format(long timestamp){
        System.out.println(timestamp);
        Date date = new Date(timestamp);
        SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = yyyyMMddHHmmss.format(date);
        System.out.println(format);
    }
}
