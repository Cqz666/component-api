package com.cqz.component.flink.demo;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

public class KeySelectorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<Tuple3<Integer, Long, String>> streamSource = env.fromElements(
                Tuple3.of(0, 10L, "a"),
                Tuple3.of(1, 10L, "b"),
                Tuple3.of(2, 10L, "c"),
                Tuple3.of(3, 20L, "d"),
                Tuple3.of(4, 30L, "e"),
                Tuple3.of(5, 40L, "f"),
                Tuple3.of(6, 50L, "g"),
                Tuple3.of(7, 50L, "h")
        );
        streamSource.keyBy((KeySelector<Tuple3<Integer, Long, String>, Integer>) value -> value.f0)
        .process(new KeyedProcessFunction<Integer, Tuple3<Integer, Long, String>, String>() {
            @Override
            public void processElement(Tuple3<Integer, Long, String> value, Context ctx, Collector<String> out) throws Exception {
                System.out.println("key = "+ctx.getCurrentKey());
//                System.out.println("emit = "+value.f2);
                int  subtaskIndex = (MathUtils.murmurHash(ctx.getCurrentKey().hashCode()) % 128) * 8 / 128;
                System.out.println("=================="+subtaskIndex);
                out.collect(value.f2);
            }
        }).setParallelism(8)
                .print();

        env.execute();
    }
}
