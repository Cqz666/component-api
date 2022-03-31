package com.cqz.component.flink.sql.parse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

public class JsonPlanTest2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        TableEnvironmentImpl tableEnvironment = (TableEnvironmentImpl) tableEnv;

        String srcTableDdl =
                "CREATE TABLE source_table (\n" +
                        "    age STRING,\n" +
                        "    sex STRING,\n" +
                        "    user_id BIGINT,\n" +
                        "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                        "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'datagen',\n" +
                        "  'rows-per-second' = '1',\n" +
                        "  'fields.age.length' = '1',\n" +
                        "  'fields.sex.length' = '1',\n" +
                        "  'fields.user_id.min' = '1',\n" +
                        "  'fields.user_id.max' = '100000'\n" +
                        ")";
        tableEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE sink_table (\n" +
                        "    window_end bigint,\n" +
                        "    age STRING,\n" +
                        "    sex STRING,\n" +
                        "    uv BIGINT\n" +
                        ") WITH (\n" +
                        "  'connector' = 'print'\n" +
                        ")";
        tableEnv.executeSql(sinkTableDdl);

        String jsonPlan = tableEnvironment.getJsonPlan("insert into sink_table\n" +
                "SELECT \n" +
                "    UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n" +
                "    if (age is null, 'ALL', age) as age,\n" +
                "    if (sex is null, 'ALL', sex) as sex,\n" +
                "    count(distinct user_id) as uv\n" +
                "FROM TABLE(CUMULATE(\n" +
                "       TABLE source_table\n" +
                "       , DESCRIPTOR(row_time)\n" +
                "       , INTERVAL '5' SECOND\n" +
                "       , INTERVAL '1' DAY))\n" +
                "GROUP BY \n" +
                "    window_start, \n" +
                "    window_end,\n" +
                "    GROUPING SETS (\n" +
                "        ()\n" +
                "        , (age)\n" +
                "        , (sex)\n" +
                "        , (age, sex)\n" +
                "    )");
        System.out.println(jsonPlan);
    }
}
