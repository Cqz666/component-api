package com.cqz.component.flink.sql.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EnvironmentUtil {

    public static TableEnvironment getBatchTableEnv(){
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .useBlinkPlanner()
                .build();
        return TableEnvironment.create(settings);
    }

    public static StreamTableEnvironment getStreamTableEnv(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return StreamTableEnvironment.create(env);
    }

}
