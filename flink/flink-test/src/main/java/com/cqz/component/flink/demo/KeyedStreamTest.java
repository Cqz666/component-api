package com.cqz.component.flink.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class KeyedStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Tuple2<Integer, String>> streamSource = env.fromElements(
                Tuple2.of(0, "a"),
                Tuple2.of(1 , "b"),
                Tuple2.of(2,  "c"),
                Tuple2.of(2, "c2"),
                Tuple2.of(3, "c2"),
                Tuple2.of(3, "c2"),
                Tuple2.of(3, "c2"),
                Tuple2.of(4, "c2"),
                Tuple2.of(5, "c2"),
                Tuple2.of(5, "1")
        );
        KeyedStream<Tuple2<Integer, String>, Integer> keyedStream = streamSource.keyBy(t -> t.f0);
        keyedStream.addSink(new RichSinkFunction<Tuple2<Integer, String>>() {
            int indexOfThisSubtask;
            @Override
            public void open(Configuration parameters) throws Exception {
                 indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
                System.out.println(indexOfThisSubtask+"----"+value.toString());
            }
        });
        env.execute();
    }
}
