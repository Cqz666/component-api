package com.cqz.component.flink.api.interval_join.datastream;

import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.util.Properties;

@Slf4j
public class IntervalJoinByDataStream {
    static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    static String kafkaBrokers = "172.21.0.2:9092";
     static String browseTopic = "browse";
     static String clickTopic = "click";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers",kafkaBrokers);
        browseProperties.put("group.id","browseTopicGroupID");

        DataStream<UserBrowseLog> browseStream=env
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new BrowseBoundedOutOfOrdernessTimestampExtractor());

        Properties clickProperties = new Properties();
        clickProperties.put("bootstrap.servers",kafkaBrokers);
        clickProperties.put("group.id","clickTopicGroupID");
        DataStream<UserClickLog> clickStream = env
                .addSource(new FlinkKafkaConsumer<>(clickTopic, new SimpleStringSchema(), clickProperties))
                .process(new ClickKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new ClickBoundedOutOfOrdernessTimestampExtractor());

        clickStream
                .keyBy("userID")
                .intervalJoin(browseStream.keyBy("userID"))
                // 时间间隔,设定下界和上界
                // 下界: 10分钟前，上界: 当前EventTime时刻
                .between(Time.minutes(-10),Time.seconds(0))
                // 自定义ProcessJoinFunction 处理Join到的元素
                .process(new ProcessJoinFunction<UserClickLog, UserBrowseLog, String>() {
                    @Override
                    public void processElement(UserClickLog left, UserBrowseLog right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left +" =Interval Join=> "+right);
                    }
                })
                .print();

        env.execute();

    }

    /**
     * 解析Kafka数据
     */
    public static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {
                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);
                if(log!=null){
                    out.collect(log);
                }
            }catch (Exception ex){
                log.error("解析Kafka数据异常...",ex);
            }
        }
    }

    /**
     * 解析Kafka数据
     */
    public static class ClickKafkaProcessFunction extends ProcessFunction<String, UserClickLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserClickLog> out) throws Exception {
            try {
                UserClickLog log = JSON.parseObject(value, UserClickLog.class);
                if(log!=null){
                    out.collect(log);
                }
            }catch (Exception ex){
                log.error("解析Kafka数据异常...",ex);
            }
        }
    }

    /**
     * 提取时间戳生成水印
     */
    public static class BrowseBoundedOutOfOrdernessTimestampExtractor implements AssignerWithPeriodicWatermarks<UserBrowseLog> {
        // 当前时间戳
        long currentTimeStamp = 0L;
        // 允许的迟到数据
        long maxOutOfOrderness = 0L;
        // 当前水位线
        long currentWaterMark = Long.MIN_VALUE;

//        BrowseBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
//            super(maxOutOfOrderness);
//        }
//
//        @Override
//        public long extractTimestamp(UserBrowseLog element) {
//            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
//            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
//            return dateTime.getMillis();
//        }

        @Nullable
        @Override
        @SneakyThrows
        public Watermark getCurrentWatermark() {
            long potentialWM  = currentTimeStamp - maxOutOfOrderness;
            if (potentialWM>=currentWaterMark){
                currentWaterMark = potentialWM;
            }
            System.out.println("browse当前水位线:" + dateTimeFormatter.print(currentWaterMark));
            Thread.sleep(3000);
            return new Watermark(currentWaterMark);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element, long recordTimestamp) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
            long timeStamp = dateTime.getMillis();
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.out.println("EventTime:" + timeStamp + ",browse水位线:" + dateTimeFormatter.print(currentWaterMark));
            return dateTime.getMillis();
        }
    }

    /**
     * 提取时间戳生成水印
     */
    public    static class ClickBoundedOutOfOrdernessTimestampExtractor implements AssignerWithPeriodicWatermarks<UserClickLog> {
        // 当前时间戳
        long currentTimeStamp = 0L;
        // 允许的迟到数据
        long maxOutOfOrderness = 0L;
        // 当前水位线
        long currentWaterMark;

//        @Override
//        public long extractTimestamp(UserClickLog element) {
//            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
//            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
//            return dateTime.getMillis();
//        }

        @SneakyThrows
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            long potentialWM  = currentTimeStamp - maxOutOfOrderness;
            if (potentialWM>=currentWaterMark){
                currentWaterMark = potentialWM;
            }
            System.out.println("click当前水位线:" + dateTimeFormatter.print(currentWaterMark));
            Thread.sleep(3000);
            return new Watermark(currentWaterMark);
        }

        @Override
        public long extractTimestamp(UserClickLog element, long recordTimestamp) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
            long timeStamp = dateTime.getMillis();
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.out.println("EventTime:" + timeStamp + ",click水位线:" + dateTimeFormatter.print(currentWaterMark));
            return dateTime.getMillis();
        }
    }

}
