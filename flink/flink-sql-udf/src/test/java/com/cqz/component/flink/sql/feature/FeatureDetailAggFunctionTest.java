package com.cqz.component.flink.sql.feature;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

public class FeatureDetailAggFunctionTest {

    StreamTableEnvironment tableEnv;

    @Before
    public void setUp() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tableEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void test(){
        tableEnv.executeSql("create table source(\n" +
                "vid varchar,\n" +
                "receive_timestamp bigint,\n" +
                "game_id varchar)\n" +
                "with(\n" +
                "'connector' = 'filesystem',\n" +
                "'path'='/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/src/main/resources/1.csv',\n" +
                "'format'='csv'\n" +
                ")");
        tableEnv.createTemporarySystemFunction("FeatureDetailAggFunction", FeatureDetailAggFunction.class);
        tableEnv.executeSql("create table sink(res varchar)with('connector' = 'print')");
        tableEnv.executeSql("INSERT INTO sink SELECT FeatureDetailAggFunction(\n" +
                "'feature_id',\n" +
                "'129600',\n" +
                "cast(vid AS VARCHAR),  \n" +
                "cast(receive_timestamp AS VARCHAR),\n" +
                "cast(game_id AS VARCHAR)\n" +
                ") as res\n" +
                "FROM source WHERE vid IS NOT NULL AND vid <> '' GROUP BY vid");
    }


}