package com.cqz.component.flink.sql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MyConcatFunction  extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... fields) {
        return Arrays.stream(fields)
                .map(Object::toString)
                .collect(Collectors.joining(","));
    }

}
