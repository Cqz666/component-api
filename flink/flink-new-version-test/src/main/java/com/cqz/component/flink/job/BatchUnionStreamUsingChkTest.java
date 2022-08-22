package com.cqz.component.flink.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BatchUnionStreamUsingChkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> boundedSource = env.fromElements(100,200,300);
        DataStreamSource<Integer> unboundedSource = env.addSource(new SourceFunction<Integer>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                Random random = new Random();
                while (flag) {
                    int i = random.nextInt(10);
                    sourceContext.collect(i);
                    TimeUnit.SECONDS.sleep(1);
                }
            }
            @Override
            public void cancel() {
                flag = false;
            }
        });
        DataStream<Integer> union = boundedSource.union(unboundedSource);
        union.print();

        env.execute();
    }
}
