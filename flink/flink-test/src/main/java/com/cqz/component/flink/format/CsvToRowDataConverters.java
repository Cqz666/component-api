package com.cqz.component.flink.format;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CsvToRowDataConverters {
    public static void main(String[] args) {
        boolean b = Boolean.parseBoolean("");
//        int i = Integer.parseInt("");
//        double v = Double.parseDouble("");
//        float v1 = Float.parseFloat("");
//        Date date = Date.valueOf("");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        LocalDateTime parse = LocalDateTime.parse("", formatter);
    }
}
