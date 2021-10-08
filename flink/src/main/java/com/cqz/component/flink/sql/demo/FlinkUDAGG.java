package com.cqz.component.flink.sql.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class FlinkUDAGG {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("CREATE TEMPORARY TABLE MyTable (myField String,`value` bigint,weight int)" +
                "with('connector' = 'filesystem'," +
                "'path' = 'D:\\Project\\component-api\\flink\\src\\main\\resources\\agg.txt'," +
                "'format' = 'csv'" +
                ")");

        // register function
        tableEnv.createTemporarySystemFunction("my_agg", WeightedAvg.class);
        Table table = tableEnv.sqlQuery(
                "SELECT myField, my_agg(`value`, weight) as cnt FROM MyTable GROUP BY myField"
        );

        TableResult execute = table.execute();
        execute.print();

    }

    // mutable accumulator of structured type for the aggregate function
    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    // function that takes (value BIGINT, weight INT), stores intermediate results in a structured
// type of WeightedAvgAccumulator, and returns the weighted average as BIGINT
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        @Override
        public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }

        public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum += iValue * iWeight;
            System.out.println(acc.sum);
            acc.count += iWeight;
            System.out.println(acc.count);

        }

        public void retract(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;

        }

        public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
            for (WeightedAvgAccumulator a : it) {
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(WeightedAvgAccumulator acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }


}
