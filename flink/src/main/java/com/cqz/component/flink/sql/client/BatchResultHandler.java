package com.cqz.component.flink.sql.client;

import com.cqz.component.flink.sql.demo.FlinkSQLDemo;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class BatchResultHandler {
    private static String DATA_PATH = FlinkSQLDemo.class.getClassLoader().getResource("data.txt").toString();

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String ddl = "CREATE TABLE tb_socket (id int,name String)" +
                "with('connector' = 'filesystem'," +
                "'path' = '"+DATA_PATH+"'," +
                "'format' = 'csv'" +
                ")";
        String ddl2 = "CREATE TABLE sink_kafka (cnt bigint,id int)" +
                "with('connector' = 'kafka'," +
                "  'topic' = 'test',"+
                " 'properties.bootstrap.servers' = 'localhost:9092'," +
                " 'properties.group.id' = 'test1'," +
                " 'format' = 'csv'," +
                " 'scan.startup.mode' = 'earliest-offset'" +
                ")";
        String dql= "insert into sink_kafka select count(*) as cnt,id from tb_socket group by id";
        //注册表
        tEnv.executeSql(ddl);
        tEnv.executeSql(ddl2);
        tEnv.executeSql(dql);

    }
}
