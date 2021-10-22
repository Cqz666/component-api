//package com.cqz.component.flink.sql.demo;
//
//import com.cqz.component.flink.sql.udf.SumFunction;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableResult;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//
//public class FlinkSQLDemo {
//
//    private static String path=FlinkSQLDemo.class.getClassLoader().getResource("data.txt").toString();
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
//
//        tableEnv.executeSql("CREATE TEMPORARY TABLE MyTable (id int,myField String)" +
//                "with('connector' = 'filesystem'," +
//                "'path' = '"+path+"'," +
//                "'format' = 'csv'" +
//                ")");
//
//        // register function
////        tableEnv.createTemporarySystemFunction("my_substring", SubstringFunction.class);
//        tableEnv.createTemporarySystemFunction("my_sum", SumFunction.class);
//
//        // call registered function in SQL
////        Table table = tableEnv.sqlQuery("SELECT id,my_substring(myField, 3,6 ) FROM MyTable");
//        Table table = tableEnv.sqlQuery("SELECT id,my_sum(id,2 ) as cnt FROM MyTable");
//
//        TableResult result = table.execute();
//
//        result.print();
//
////        env.execute();
//
//    }
//}
