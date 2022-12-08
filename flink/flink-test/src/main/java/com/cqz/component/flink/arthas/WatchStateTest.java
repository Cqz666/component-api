package com.cqz.component.flink.arthas;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class WatchStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        DataStreamSource<Tuple2<String, Long>> tupleDS = env.addSource(new UserDefinedSource());
         tupleDS.keyBy(t -> t.f0)
                .map(new WatchStateFunc())
                .print();
        env.execute();
    }

    private static class UserDefinedSource implements SourceFunction<Tuple2<String, Long>> {

        private volatile boolean isCancel;
        private static final String name[] = {"A","B","C","D"};

        @Override
        public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
            Random random = new Random();
            while (!this.isCancel) {
                int i = random.nextInt(4);
                long l = random.nextInt(100000);
                sourceContext.collect(Tuple2.of(name[i], l));
                Thread.sleep(1000L);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
