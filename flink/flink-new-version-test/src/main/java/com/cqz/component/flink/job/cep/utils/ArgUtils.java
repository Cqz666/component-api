package com.cqz.component.flink.job.cep.utils;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import static com.cqz.component.flink.job.cep.utils.Constants.*;

public class ArgUtils {

    public static void checkArg(String argName, MultipleParameterTool params) {
        if (!params.has(argName)) {
            throw new IllegalArgumentException(argName + " must be set!");
        }
    }

    public static void check(MultipleParameterTool params){
        checkArg(KAFKA_BROKERS_ARG, params);
        checkArg(INPUT_TOPIC_ARG, params);
        checkArg(INPUT_TOPIC_GROUP_ARG, params);
        checkArg(JDBC_URL_ARG, params);
        checkArg(TABLE_NAME_ARG, params);
        checkArg(JDBC_INTERVAL_MILLIS_ARG, params);
    }


}
