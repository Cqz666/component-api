package com.cqz.component.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SubstringFunction extends ScalarFunction {

    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }

}
