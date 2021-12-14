package com.cqz.component.flink.sql.client;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class Test2SinkPrint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        String source = "CREATE TABLE `source` (" +
                "    order_number INT," +
                "    price        DECIMAL(32,2)," +
                "    order_time   TIMESTAMP(0)" +
                ") WITH (" +
                "  'connector' = 'datagen'" +
                ")";

        String sink_1 = "CREATE TABLE sink_1 (" +
                "    order_number INT," +
                "    price        DECIMAL(32,2)," +
                "    order_time   TIMESTAMP(0)" +
                ") WITH (" +
                "  'connector' = 'my_preview'," +
                "'max-number-of-outputs-per-second'='5',"+
                "'print-identifier'='sink_1'"+
                ")";

        String sink_2 = "CREATE TABLE sink_2 (" +
                "    order_number INT," +
                "    price        DECIMAL(32,2)," +
                "    order_time   TIMESTAMP(0)" +
                ") WITH (" +
                "  'connector' = 'my_preview'," +
                "'max-number-of-outputs-per-second'='5',"+
                "'print-identifier'='sink_2'"+
                ")";

        String ins1 = "insert into sink_1 select * from source";
        String ins2 = "insert into sink_2 select * from source";

        String qry = "select * from source";

        tEnv.executeSql(source);
        tEnv.executeSql(sink_1);
        tEnv.executeSql(sink_2);

        tEnv.executeSql(ins1);
        tEnv.executeSql(ins2);

        TableResult result = tEnv.executeSql(qry);

        try(CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()){
                final Row next = it.next();
                
            }
        }

    }
}
