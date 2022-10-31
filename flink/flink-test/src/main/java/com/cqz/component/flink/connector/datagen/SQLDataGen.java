package com.cqz.component.flink.connector.datagen;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;
import java.util.Map;

public class SQLDataGen {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableEnvironmentImpl tableEnvImpl = (TableEnvironmentImpl) tableEnv;
        CatalogManager catalogManager = tableEnvImpl.getCatalogManager();
        tableEnv.executeSql("CREATE TABLE Orders (\n" +
                "    id INT,\n" +
                "    game_id STRING,\n" +
                "    user_id        BIGINT,\n" +
                "    total_amount   DOUBLE,\n" +
                "    create_time   TIMESTAMP(3),\n" +
                "    is_balck BOOLEAN,\n"+
                "    WATERMARK FOR create_time AS create_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second'='5',\n" +
                "  'number-of-rows'='100',\n" +
                "  'fields.id.kind'='sequence' ,\n" +
                "  'fields.id.start'='1',\n" +
                "  'fields.id.end'='1000000',\n" +
                "  'fields.game_id.kind'='random',\n" +
//                "  'fields.game_id.length'='3',\n" +
                "  'fields.game_id.start'='1',\n" +
//                "  'fields.name.end'='1000',\n" +
                "  'fields.user_id.kind'='random',\n" +
                "  'fields.user_id.min'='1',\n" +
                "  'fields.user_id.max'='20',\n" +
                "  'fields.total_amount.kind'='random',\n" +
                "  'fields.total_amount.min'='1',\n" +
                "  'fields.total_amount.max'='1000000'\n" +
                ")\n");

        Table table = tableEnv.sqlQuery("select * from Orders");
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
        DataStream<String> json = rowDataStream.map(new RichMapFunction<Row, String>() {
             private Map<String, Object> targetMap = new LinkedHashMap<>();
            @Override
            public String map(Row value) throws Exception {
                for (String fieldName : value.getFieldNames(true)) {
                    targetMap.put(fieldName, value.getField(fieldName));
                }
                return JSONObject.toJSONString(targetMap);
            }

            @Override
            public void close() throws Exception {
                targetMap = null;
            }

        });
        json.print();
        env.execute();


    }
}
