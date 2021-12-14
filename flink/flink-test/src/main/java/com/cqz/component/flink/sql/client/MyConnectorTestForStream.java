package com.cqz.component.flink.sql.client;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MyConnectorTestForStream {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        String source = "CREATE TABLE tb_source (" +
                "    order_number INT," +
                "    price        DECIMAL(32,2)," +
                "    buyer        ROW<first_name STRING, last_name STRING>," +
                "    order_time   TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'datagen'" +
                ")";
//                String source = "CREATE TABLE tb_source (" +
//                "    order_number INT," +
//                "    price        DECIMAL(32,2)," +
//                "    order_time   TIMESTAMP(3)" +
//                        ")with('connector' = 'filesystem'," +
//                        "'path' = '"+DATA_PATH+"'," +
//                        "'format' = 'csv'" +
//                        ")";

        String sink = "CREATE TABLE tb_sink (" +
                "    order_number INT," +
                "    price        DECIMAL(32,2)," +
                "    order_time   TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'print'" +
                ")";

        String count_sink = "CREATE TABLE tb_sink_count (" +
                "    order_number INT," +
                "    cnt        BIGINT" +
                ") WITH (" +
                "  'connector' = 'my_preview'," +
                "'max-number-of-outputs-per-second'='5'"+
//                "  'number-of-rows' = '10'" +
                ")";
        String sql = "insert into tb_sink select order_number,price,order_time from tb_source";
        String qry = "select order_number,price,order_time from tb_source";
        String count_sql = "insert into tb_sink_count select order_number,count(*) as cnt from tb_source group by order_number";
//        String show_table ="show tables";
//        String show_database ="show databases";
        //注册表
        tEnv.executeSql(source);
//        tEnv.executeSql(sink);
        TableResult result = tEnv.executeSql(qry);
        result.print();
//        TableResult result = tEnv.executeSql(show_table);
//        result.print();
//        TableResult result1 = tEnv.executeSql(show_database);
//        result1.print();
    }
}
