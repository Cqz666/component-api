package com.cqz.component.flink.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;

public class RealtimeScreenDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String topic = parameterTool.get("-topic");
        DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), buildConsumerProperties()))
                .name("source_kafka_" + topic).uid("source_kafka_" + topic);
        DataStream<SubOrderDetail> orderStream = sourceStream
                .map(message -> JSON.parseObject(message, SubOrderDetail.class))
                .name("map_sub_order_detail").uid("map_sub_order_detail");
        WindowedStream<SubOrderDetail, Tuple, TimeWindow> siteDayWindowStream = orderStream
                .keyBy("siteId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));
        DataStream<OrderAccumulator> siteAggStream = siteDayWindowStream
                .aggregate(new OrderAndGmvAggregateFunc())
                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

        DataStream<Tuple2<Long, String>> siteResultStream = siteAggStream
                .keyBy(0)
                .process(new OutputOrderGmvProcessFunc(), TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}))
                .name("process_site_gmv_changed").uid("process_site_gmv_changed");

        siteResultStream
                .addSink(new RedisSink())
                .name("sink_redis_site_gmv").uid("sink_redis_site_gmv")
                .setParallelism(1);

        WindowedStream<SubOrderDetail, Tuple, TimeWindow> merchandiseWindowStream = orderStream
                .keyBy("merchandiseId")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)));

        DataStream<Tuple2<Long, Long>> merchandiseRankStream = merchandiseWindowStream
                .aggregate(new MerchandiseSalesAggregateFunc(), new MerchandiseSalesWindowFunc())
                .name("aggregate_merch_sales").uid("aggregate_merch_sales")
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() { }));

    }

    public static final class MerchandiseSalesAggregateFunc
            implements AggregateFunction<SubOrderDetail, Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SubOrderDetail value, Long acc) {
            return acc + value.getQuantity();
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }


    public static final class MerchandiseSalesWindowFunc implements WindowFunction<Long, Tuple2<Long, Long>, Tuple, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Long> accs,
                Collector<Tuple2<Long, Long>> out) throws Exception {
            long merchId = ((Tuple1<Long>) key).f0;
            long acc = accs.iterator().next();
            out.collect(new Tuple2<>(merchId, acc));
        }
    }

    public static final class RedisSink extends RichSinkFunction {
        private static final long serialVersionUID = 1L;
    }

    public static final class OutputOrderGmvProcessFunc extends KeyedProcessFunction<Tuple, OrderAccumulator, Tuple2<Long, String>> {
        private static final long serialVersionUID = 1L;

        private MapState<Long, OrderAccumulator> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            state = this.getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "state_site_order_gmv",
                    Long.class,
                    OrderAccumulator.class)
            );
        }

        @Override
        public void processElement(OrderAccumulator value, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
            long key = value.getSiteId();
            OrderAccumulator cachedValue = state.get(key);

            if (cachedValue == null || value.getSubOrderSum() != cachedValue.getSubOrderSum()) {
                JSONObject result = new JSONObject();
                result.put("site_id", value.getSiteId());
                result.put("site_name", value.getSiteName());
                result.put("quantity", value.getQuantitySum());
                result.put("orderCount", value.getOrderIds().size());
                result.put("subOrderCount", value.getSubOrderSum());
                result.put("gmv", value.getGmv());
                out.collect(new Tuple2<>(key, result.toJSONString()));
                state.put(key, value);
            }
        }

        @Override
        public void close() throws Exception {
            state.clear();
            super.close();
        }
    }

    public static final class OrderAndGmvAggregateFunc implements AggregateFunction<SubOrderDetail, OrderAccumulator, OrderAccumulator> {
        private static final long serialVersionUID = 1L;

        @Override
        public OrderAccumulator createAccumulator() {
            return new OrderAccumulator();
        }

        @Override
        public OrderAccumulator add(SubOrderDetail record, OrderAccumulator acc) {
            if (acc.getSiteId() == 0) {
                acc.setSiteId(record.getSiteId());
                acc.setSiteName(record.getSiteName());
            }
            acc.addOrderId(record.getOrderId());
            acc.addSubOrderSum(1);
            acc.addQuantitySum(record.getQuantity());
            acc.addGmv(record.getPrice() * record.getQuantity());
            return acc;
        }

        @Override
        public OrderAccumulator getResult(OrderAccumulator acc) {
            return acc;
        }

        @Override
        public OrderAccumulator merge(OrderAccumulator acc1, OrderAccumulator acc2) {
            if (acc1.getSiteId() == 0) {
                acc1.setSiteId(acc2.getSiteId());
                acc1.setSiteName(acc2.getSiteName());
            }
            acc1.addOrderIds(acc2.getOrderIds());
            acc1.addSubOrderSum(acc2.getSubOrderSum());
            acc1.addQuantitySum(acc2.getQuantitySum());
            acc1.addGmv(acc2.getGmv());
            return acc1;
        }
    }

    public static class OrderAccumulator {
        private long siteId;
        private String siteName;
        private long quantity;
        private long subOrder;
        private long gmv;
        private HashSet<Long> orderIds = new HashSet<>();

        public long getGmv(){
            return gmv;
        }

        public void addGmv(long gmv){
            this.gmv+=gmv;
        }

        public void addQuantitySum(long quantity){
            this.quantity+=quantity;
        }

        public long getQuantitySum(){
            return quantity;
        }

        public long getSubOrderSum(){
            return subOrder;
        }

        public void addSubOrderSum(long cnt){
            subOrder+=cnt;
        }

        public void addOrderId(Long id){
            orderIds.add(id);
        }

        public void addOrderIds(HashSet<Long> ids){
            orderIds.addAll(ids);
        }
        public HashSet<Long> getOrderIds(){
            return orderIds;
        }

        public long getSiteId(){
            return siteId;
        }

        public void  setSiteId(long siteId){
            this.siteId = siteId;
        }

        public String getSiteName(){
            return siteName;
        }

        public void  setSiteName(String siteName){
            this.siteName = siteName;
        }

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class SubOrderDetail implements Serializable {
        private static final long serialVersionUID = 1L;
        private long userId;
        private long orderId;
        private long subOrderId;
        private long siteId;
        private String siteName;
        private long cityId;
        private String cityName;
        private long warehouseId;
        private long merchandiseId;
        private long price;
        private long quantity;
        private int orderStatus;
        private int isNewOrder;
        private long timestamp;
    }

    private static Properties buildConsumerProperties(){
        Properties props = new Properties();
        //docker host ip
        props.put("bootstrap.servers", "172.21.0.2:9092");
        props.put("group.id","idea-test-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("max.poll.records","5000");
        return props;
    }

}
