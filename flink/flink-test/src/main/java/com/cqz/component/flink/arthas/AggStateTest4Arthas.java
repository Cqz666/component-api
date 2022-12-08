package com.cqz.component.flink.arthas;

import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class AggStateTest4Arthas {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorRecord> source = env.addSource(new SensorRecordSource());

        source.keyBy(SensorRecord::getId)
                .process(new MyKeyedProcessFunction())
                .print();

        env.execute();
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, SensorRecord, Tuple2<String, Double>> {

        private transient AggregatingState aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            //定义描述器
            AggregatingStateDescriptor aggregatingStateDescriptor = new AggregatingStateDescriptor(
                    "avg-temp", new AvgFunc(), TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>(){})
            );
            //获取ReducingState
            aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
        }

        @Override
        public void processElement(SensorRecord value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {

            aggregatingState.add(value);
            out.collect(Tuple2.of(value.getId(), (Double) aggregatingState.get()) );
        }
    }

    public static class AvgFunc implements AggregateFunction<SensorRecord, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return Tuple2.of(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorRecord value, Tuple2<Double, Integer> accumulator) {
            Integer currentCount = accumulator.f1;
            currentCount += 1;
            accumulator.f1 = currentCount;
            return new Tuple2<>(accumulator.f0 + value.getRecord(), accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    private static class SensorRecordSource implements SourceFunction<SensorRecord> {

        private volatile boolean isCancel;
        private static final String ID[] = {"A","B","C","D"};

        public static final int records[] = {-20,-19,-18,-17,-16,-15,-14,-13,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40};

        @Override
        public void run(SourceContext<SensorRecord> sourceContext) throws Exception {
            Random random = new Random();
            while (!this.isCancel) {
                int i = random.nextInt(4);
                int j = random.nextInt(61);
                SensorRecord record = new SensorRecord();
                record.setId(ID[i]);
                record.setRecord(records[j]);
                record.setTimestamp(System.currentTimeMillis());

                sourceContext.collect(record);
                Thread.sleep(1000L);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

    @Data
    public static class SensorRecord {
        private String id;

        private Integer record;

        private Long timestamp;

    }
}
