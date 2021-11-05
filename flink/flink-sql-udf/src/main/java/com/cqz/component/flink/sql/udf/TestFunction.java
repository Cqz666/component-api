package com.cqz.component.flink.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestFunction {
    public static void main(String[] args) {
//        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment env = StreamTableEnvironment.create(senv);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment env = TableEnvironment.create(settings);

        env.createTemporarySystemFunction("SumFunction",SumFunction.class);
        env.createTemporarySystemFunction("MyConcatFunction",MyConcatFunction.class);
        env.createTemporarySystemFunction("SplitFunction",new SplitFunction(" "));
        env.createTemporarySystemFunction("WeightedAvg",WeightedAvg.class);

        //        env.executeSql("CREATE TABLE sink_print(id int) WITH ('connector'='print')");
//        env.executeSql("create table MyTable2(id int)with('connector'='datagen')");
        env.executeSql("create table MyTable(id int)with('connector'='filesystem','path'='/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/src/main/resources/data','format'='csv')");
        env.executeSql("create table MyTable2(id int,name string)with('connector'='filesystem','path'='/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/src/main/resources/data2','format'='csv')");
        env.executeSql("create table MyTable3(myField string)with('connector'='filesystem','path'='/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/src/main/resources/udtf_data','format'='csv')");
        env.executeSql("create table MyTable4(myField string,`value` bigint,weight int)with('connector'='filesystem','path'='/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/src/main/resources/agg_data','format'='csv')");

        TableResult result = env.executeSql("select SumFunction(id,10) as a from MyTable ");

        TableResult result2 = env.executeSql("select MyConcatFunction(id,name,123) as a from MyTable2 order by id");
        TableResult result3 = env.executeSql("select MyConcatFunction(id,'name',123) as a from MyTable2 order by id");

        TableResult result4 = env.executeSql("select myField ,T.word, T.length from MyTable3, LATERAL TABLE(SplitFunction(myField)) as T(word,length)");
        TableResult result5 = env.executeSql("select myField ,T.word, T.length from MyTable3 LEFT JOIN  LATERAL TABLE(SplitFunction(myField)) as T(word,length) ON TRUE");

        TableResult result6 = env.executeSql("select myField ,WeightedAvg(`value`,weight) from MyTable4 group by myField");

        result.print();
        result2.print();
        result3.print();
        result4.print();
        result5.print();
        result6.print();
    }
}
