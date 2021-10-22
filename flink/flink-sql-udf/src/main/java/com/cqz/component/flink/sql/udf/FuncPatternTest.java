package com.cqz.component.flink.sql.udf;

import java.util.regex.Pattern;

public class FuncPatternTest {
    private static final String FUNC_PATTERN_STR =
            "(?i)\\s*CREATE\\s+(scalar|table|aggregate)\\s+FUNCTION\\s+(\\S+)\\s+WITH\\s+(\\S+)";

    private static final Pattern FUNC_PATTERN = Pattern.compile(FUNC_PATTERN_STR);


    public static void main(String[] args) {
        String stmt = "CREATE  table FUNCTION my_sum WITH 'com.cqz.component.flink.sql.udf.SumFunction';";
        if (FUNC_PATTERN.matcher(stmt).find()) {
            System.out.println(1);
        }
        }
}
