package com.cqz.component.flink.sql.client;

import com.cqz.component.flink.sql.utils.EnvironmentUtil;
import org.apache.flink.table.api.TableEnvironment;

public class MyConnectorTestBatch {
//    private static String DATA_PATH = MyConnectorTestBatch.class.getClassLoader().getResource("data.txt").toString();

    public static void main(String[] args) {

        TableEnvironment tEnv = EnvironmentUtil.getBatchTableEnv();

        String ddl = "CREATE TABLE tb_socket (id int,name String)" +
                "with('connector' = 'filesystem'," +
//                "'path' = '"+DATA_PATH+"'," +
                "'path' = 'hdfs:///tmp/chenqizhu/data.txt'," +
                "'format' = 'csv'" +
                ")";

        String ddl3 = "CREATE TABLE sink_print(id int,cnt bigint) WITH ('connector'='my_preview')";
//        String ddl3 = "CREATE TABLE sink_print(id int,cnt bigint) WITH ('connector'='print','sink.parallelism'='1')";
        String dql= "insert into sink_print select id,count(*) as cnt from tb_socket group by id ";
        //注册表
        tEnv.executeSql(ddl);
        tEnv.executeSql(ddl3);
        tEnv.executeSql(dql);

    }
}
