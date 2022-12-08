package com.cqz.component.flink.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;

public class TumbleWindoweDmo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        WatermarkStrategy<Tuple4<String, String, Integer, Long>> tuple4WatermarkStrategy =
                WatermarkStrategy.<Tuple4<String, String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((tuple, timestamp) -> tuple.f3);

        env.addSource(new UserDefinedSource())
                .assignTimestampsAndWatermarks(tuple4WatermarkStrategy)
                .keyBy(k->k.f0+k.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(2)
                .print();

        env.execute();

    }

    private static class UserDefinedSource implements SourceFunction<Tuple4<String, String, Integer, Long>> {

        private volatile boolean isCancel;
        private static final String a[] = {"a","b","c","d","e"};
        private static final String b[] = {"A","B","C","D","E"};

        @Override
        public void run(SourceContext<Tuple4<String, String, Integer, Long>> sourceContext) throws Exception {
            Random random = new Random();
            while (!this.isCancel) {
                int i = random.nextInt(5);
                int j = random.nextInt(5);
                sourceContext.collect(Tuple4.of(a[i], b[j], 1, System.currentTimeMillis()));
                Thread.sleep(10L);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
