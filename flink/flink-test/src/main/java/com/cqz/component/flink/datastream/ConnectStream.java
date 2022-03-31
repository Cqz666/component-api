package com.cqz.component.flink.datastream;

import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class ConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        KeyedStream<SourceModel, Integer> keyedStream1 = env.addSource(new SourceFunction<SourceModel>() {
            private volatile boolean isRun=true;
            @Override
            public void run(SourceContext<SourceModel> ctx) throws Exception {
                while (isRun) {
                    ctx.collect(
                            SourceModel
                                    .builder()
                                    .id(RandomUtils.nextInt(0, 1000))
                                    .price(RandomUtils.nextInt(0, 100))
                                    .timestamp(System.currentTimeMillis())
                                    .build()
                    );
                    Thread.sleep(10L);
                }
            }
            @Override
            public void cancel() {
                isRun = false;
            }
        }).keyBy(new KeySelector<SourceModel, Integer>() {
            @Override
            public Integer getKey(SourceModel sourceModel) throws Exception {
                return sourceModel.getId();
            }
        });

        KeyedStream<SourceModel2, Integer> keyedStream2 = env.addSource(new SourceFunction<SourceModel2>() {
            private volatile boolean isRun=true;

            @Override
            public void run(SourceContext<SourceModel2> ctx) throws Exception {
                while (isRun) {
                    ctx.collect(
                            SourceModel2
                                    .builder()
                                    .id(RandomUtils.nextInt(0, 1000))
                                    .name(String.valueOf(RandomUtils.nextInt(0, 100)))
                                    .timestamp(System.currentTimeMillis())
                                    .build()
                    );
                    Thread.sleep(10L);
                }
            }

            @Override
            public void cancel() {
                isRun = false;
            }
        }).keyBy(new KeySelector<SourceModel2, Integer>() {
            @Override
            public Integer getKey(SourceModel2 sourceModel2) throws Exception {
                return sourceModel2.getId();
            }
        });

        SingleOutputStreamOperator<SinkModel> unionStream = keyedStream1.connect(keyedStream2)
                .process(new KeyedCoProcessFunction<Integer, SourceModel, SourceModel2, SinkModel>() {

                    private transient MapState<Integer, SourceModel> source1State;
                    private transient MapState<Integer, SourceModel2> source2State;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.source1State = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("a", TypeInformation.of(Integer.class), TypeInformation.of(SourceModel.class)));
                        this.source2State = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("b", TypeInformation.of(Integer.class), TypeInformation.of(SourceModel2.class)));
                    }

                    @Override
                    public void processElement1(SourceModel value, Context ctx, Collector<SinkModel> out) throws Exception {
                        this.source1State.put(value.getId(), value);
                        SourceModel2 sourceModel2 = source2State.get(value.getId());
                        String name = null;
                        if (sourceModel2!=null){
                            name =  source2State.get(value.getId()).getName();
                        }
                        out.collect(SinkModel.builder()
                                .id(value.getId())
                                .name(name)
                                .price(value.getPrice())
                                .timestamp(value.getTimestamp())
                                .build());
                    }

                    @Override
                    public void processElement2(SourceModel2 value, Context ctx, Collector<SinkModel> out) throws Exception {
                        this.source2State.put(value.getId(), value);
                        SourceModel sourceModel = source1State.get(value.getId());
                        int price=-1;
                        if (sourceModel!=null){
                            price =  source1State.get(value.getId()).getPrice();
                        }

                        out.collect(SinkModel.builder()
                                .id(value.getId())
                                .name(value.getName())
                                .price(price)
                                .timestamp(value.getTimestamp())
                                .build());
                    }

                });
        unionStream.keyBy(new KeySelector<SinkModel, Integer>() {
            @Override
            public Integer getKey(SinkModel sinkModel) throws Exception {
                return sinkModel.getId();
            }
        }).addSink(new SinkFunction<SinkModel>() {
            @Override
            public void invoke(SinkModel value, Context context) throws Exception {
                System.out.println(value.toString());
            }
        });

        env.execute();
    }

    @Data
    @Builder
    private static class SourceModel {
        private int id;
        private int price;
        private long timestamp;
    }

    @Data
    @Builder
    private static class SourceModel2 {
        private int id;
        private String name;
        private long timestamp;
    }

    @Data
    @Builder
    private static class SinkModel {
        private int id;
        private int price;
        private String name;
        private long timestamp;
    }

}
