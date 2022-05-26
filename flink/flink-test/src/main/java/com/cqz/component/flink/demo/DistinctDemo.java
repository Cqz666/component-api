package com.cqz.component.flink.demo;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DistinctDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple3<Integer, Long, String>> streamSource = env.fromElements(
                Tuple3.of(1, 10L, "a"),
                Tuple3.of(1, 10L, "a"),
                Tuple3.of(1, 10L, "a"),
                Tuple3.of(2, 20L, "b"),
                Tuple3.of(3, 30L, "c"),
                Tuple3.of(4, 40L, "d"),
                Tuple3.of(5, 50L, "e")
        );
        DataStream<String> dedupStream = streamSource
                .keyBy(1)
                .process(new SubOrderDeduplicateProcessFunc(), TypeInformation.of(String.class))
                .name("process_sub_order_dedup")
                .uid("process_sub_order_dedup");

        dedupStream.print();
        env.execute();

    }

    // 去重用的ProcessFunction
    public static final class SubOrderDeduplicateProcessFunc extends KeyedProcessFunction<Tuple, Tuple3<Integer, Long, String>, String> {
        private static final long serialVersionUID = 1L;

        private ValueState<Boolean> existState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .cleanupInRocksdbCompactFilter(10000)
                    .build();

            ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<>(
                    "suborder-dedup-state",
                    Boolean.class
            );
            existStateDesc.enableTimeToLive(stateTtlConfig);

            existState = this.getRuntimeContext().getState(existStateDesc);
        }

        @Override
        public void processElement(Tuple3<Integer, Long, String> value, Context ctx, Collector<String> out) throws Exception {
            if (existState.value() == null) {
                existState.update(true);
                out.collect(value.f2);
            }
        }
    }


}
