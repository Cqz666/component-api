package com.cqz.component.flink.job;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class ExceptionsTestJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = env.addSource(new MyStreamingSource());
        source.map( value -> 24/value).print();

        env.execute();
    }

     static class MyStreamingSource implements SourceFunction<Integer> {

         private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            List<Integer> list = Arrays.asList(0,2,4,6,8);
            while (isRunning) {
                int i = new Random().nextInt(5);
                ctx.collect(list.get(i));
                //每秒产生一条数据
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
