package com.cqz.component.flink.sql.client;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String source ="create table sourceTable" +
                "(" +
                "`region` varchar," +
                "deviceName varchar," +
                "collectValue double," +
                "eventTime bigint," +
                "ts as to_timestamp(from_unixtime(eventTime,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for ts as ts - interval '5' second" +
                ") WITH(" +
                "'connector'='kafka'," +
                "'topic' = 'firsttopic'," +
                "'properties.group.id' = 'start_log_group'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'format'='csv'," +
                "'csv.ignore-parse-errors' = 'true'," +
                "'csv.allow-comments' = 'true'" +
                ")";

        String sink = "create table destinationTable" +
                "(" +
                "`region` varchar," +
                "deviceName varchar," +
                "collectValue double" +
                ") WITH(" +
                "'connector'='kafka'," +
                "'topic' = 'secondtopic'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'format'='json'" +
                ")";

        String sql = "insert into destinationTable " +
                "select * from sourceTable where deviceName <> '' and `region` <> ''";
        String sql2 = "show tables";
        tEnv.executeSql(source);
        tEnv.executeSql(sink);
        tEnv.executeSql(sql2);

    }
}
