package com.cqz.component.flink.demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//方便观察

        //2.Source
//        DataStreamSource<Tuple2<String, Long>> tupleDS = env.fromElements(
//                Tuple2.of("北京", 1L),
//                Tuple2.of("上海", 2L),
//                Tuple2.of("北京", 6L),
//                Tuple2.of("上海", 8L),
//                Tuple2.of("北京", 3L),
//                Tuple2.of("上海", 4L)
//        );

        DataStreamSource<Tuple2<String, Long>> tupleDS = env.addSource(new UserDefinedSource());

        //3.Transformation
        //使用KeyState中的ValueState获取流数据中的最大值(实际中直接使用maxBy即可)
        //实现方式1:直接使用maxBy--开发中使用该方式即可
        //min只会求出最小的那个字段,其他的字段不管
        //minBy会求出最小的那个字段和对应的其他的字段
        //max只会求出最大的那个字段,其他的字段不管
        //maxBy会求出最大的那个字段和对应的其他的字段
        SingleOutputStreamOperator<Tuple2<String, Long>> result = tupleDS.keyBy(t -> t.f0)
                .maxBy(1);

        //实现方式2:使用KeyState中的ValueState---学习测试时使用,或者后续项目中/实际开发中遇到复杂的Flink没有实现的逻辑,才用该方式!
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result2 = tupleDS.keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
                    //-1.定义状态用来存储最大值
                    private ValueState<Long> maxValueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //-2.定义状态描述符:描述状态的名称和里面的数据类型
                        ValueStateDescriptor descriptor = new ValueStateDescriptor("maxValueState", Long.class);
                        //-3.根据状态描述符初始化状态
                        maxValueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                        //-4.使用State,取出State中的最大值/历史最大值
                        Long historyMaxValue = maxValueState.value();
                        Long currentValue = value.f1;
                        if (historyMaxValue == null || currentValue > historyMaxValue) {
                            //5-更新状态,把当前的作为新的最大值存到状态中
                            maxValueState.update(currentValue);
                            return Tuple3.of(value.f0, currentValue, currentValue);
                        } else {
                            return Tuple3.of(value.f0, currentValue, historyMaxValue);
                        }
                    }
                });


        //4.Sink
//        result.print("-----");
        result2.print("====");

        //5.execute
        env.execute();
    }

    private static class UserDefinedSource implements SourceFunction<Tuple2<String, Long>> {

        private volatile boolean isCancel;
        private static final String city[] = {"北京","上海","广州","深圳"};

        @Override
        public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
            Random random = new Random();
            while (!this.isCancel) {
                int i = random.nextInt(4);
                long l = random.nextInt(100000);
                sourceContext.collect(Tuple2.of(city[i], l));
                Thread.sleep(1000L);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
