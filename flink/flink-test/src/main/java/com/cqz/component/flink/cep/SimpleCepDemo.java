package com.cqz.component.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class SimpleCepDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DataStream<EventMsg> dataStream =
                env.fromElements(
                        new EventMsg(1L, LocalDateTime.parse("2020-04-15 08:05:01", dateTimeFormatter), "A", "INFO"),
                        new EventMsg(2L, LocalDateTime.parse("2020-04-15 08:06:11", dateTimeFormatter), "A", "error"),
                        new EventMsg(3L, LocalDateTime.parse("2020-04-15 08:07:21", dateTimeFormatter), "A", "critical"),
                        new EventMsg(4L, LocalDateTime.parse("2020-04-15 08:08:21", dateTimeFormatter), "A", "INFO"),
                        new EventMsg(5L, LocalDateTime.parse("2020-04-15 08:09:21", dateTimeFormatter), "B", "INFO"),
                        new EventMsg(6L, LocalDateTime.parse("2020-04-15 08:11:51", dateTimeFormatter), "B", "error"),
                        new EventMsg(7L, LocalDateTime.parse("2020-04-15 08:12:20", dateTimeFormatter), "B", "critical"),
                        new EventMsg(8L, LocalDateTime.parse("2020-04-15 08:15:22", dateTimeFormatter), "B", "INFO"),
                        new EventMsg(9L, LocalDateTime.parse("2020-04-15 08:17:34", dateTimeFormatter), "B", "error"),
                        new EventMsg(10L, LocalDateTime.parse("2020-04-15 08:17:35", dateTimeFormatter), "B", "error"),
                        new EventMsg(11L, LocalDateTime.parse("2020-04-15 08:18:21", dateTimeFormatter), "B", "critical")
                );
//                        new EventMsg(8L, LocalDateTime.parse("2020-04-15 08:18:01", dateTimeFormatter), "B", "critical"));

                SingleOutputStreamOperator<EventMsg> watermarks = dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventMsg>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> toEpochMilli(element.getEventTime())));

        Pattern<EventMsg, ?> pattern = Pattern.<EventMsg>begin("start")
                .next("middle").where(new SimpleCondition<EventMsg>() {
                    @Override
                    public boolean filter(EventMsg value) throws Exception {
                        return value.getEventType().equals("error");
                    }
                }).followedBy("end").where(new SimpleCondition<EventMsg>() {
                    @Override
                    public boolean filter(EventMsg value) throws Exception {
                        return value.getEventType().equals("critical");
                    }
                }).within(Time.seconds(180));

        PatternStream<EventMsg> patternStream = CEP.pattern(watermarks, pattern);

        DataStream<String> alerts = patternStream.select(new PatternSelectFunction<EventMsg, String>() {
            @Override
            public String select(Map<String, List<EventMsg>> msgs) throws Exception {
                StringBuffer sb = new StringBuffer();
                msgs.forEach((k,v)->{
                    sb.append(k+",");
                    sb.append(v.toString()+"\n");
                });
                return sb.toString();
            }
        });

        alerts.print();
        env.execute("Flink CEP Test");
    }

    public static final ZoneOffset zoneOffset8 = ZoneOffset.of("+8");

    public static long toEpochMilli(LocalDateTime dt) {
        return dt.toInstant(zoneOffset8).toEpochMilli();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventMsg {
        public long eventId;
        public LocalDateTime eventTime;
        public String eventName;
        public String eventType;

        @Override
        public String toString(){
            return String.format("%s-%s-%s-%s",eventId,eventName,eventType,eventTime);
        }
    }

}
