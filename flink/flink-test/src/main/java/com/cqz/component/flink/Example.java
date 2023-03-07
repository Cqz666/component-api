package com.cqz.component.flink;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.*;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<String> elments ;
        if (args.length==0){
            elments = new ArrayList<>(Collections.singletonList("没有参数输入"));
        }else {
            elments = new ArrayList<>(Arrays.asList(args));
        }

        DataStream<String> source = env.fromCollection(elments);
        DataStream<Tuple4<String, String, Integer, Long>> source2 = env.addSource(new UserDefinedSource());
        source.print("输入的参数========>");
        source2.addSink(new SinkFunction<Tuple4<String, String, Integer, Long>>() {
            @Override
            public void invoke(Tuple4<String, String, Integer, Long> value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
            }
        });

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
                Thread.sleep(10000L);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }
}
