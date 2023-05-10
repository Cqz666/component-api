package com.cqz.component.flink.job.cep;

import com.cqz.component.flink.job.cep.condition.EndCondition;
import com.cqz.component.flink.job.cep.condition.StartCondition;
import com.cqz.component.flink.job.cep.dynamic.JDBCPeriodicPatternProcessorDiscovererFactory;
import com.cqz.component.flink.job.cep.event.Event;
import com.cqz.component.flink.job.cep.event.EventDeSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.cqz.component.flink.job.cep.utils.Constants.*;

public class CepDemo {
    public static final String JDBC_URL_DEFAULT = "jdbc:mysql://localhost:3306/cep_demo_db?user=root&password=root";
    public static final String TABLE_NAME_DEFAULT = "cep_pattern_rule";
    public static final String KAFKA_BROKERS_DEFAULT = "172.21.0.3:9092";
    public static final String INPUT_TOPIC_DEFAULT = "cep_test";

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        //ArgUtils.check(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //build kafka source
        KafkaSource<Event> kafkaSource =
                KafkaSource.<Event>builder()
                        .setBootstrapServers(params.get(KAFKA_BROKERS_ARG, KAFKA_BROKERS_DEFAULT))
                        .setTopics(params.get(INPUT_TOPIC_ARG, INPUT_TOPIC_DEFAULT))
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setGroupId(params.get(INPUT_TOPIC_GROUP_ARG,"cep_test_group"))
                        .setDeserializer(new EventDeSerializationSchema())
                        .build();
        // DataStream Source
        DataStreamSource<Event> source = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Event>forMonotonousTimestamps().
                        withTimestampAssigner((event, ts) -> event.getEventTime()), "Kafka Source");

        env.setParallelism(1);

        // keyBy userId and productionId
        KeyedStream<Event, Tuple2<Integer, Integer>> keyedStream =
                source.keyBy(new KeySelector<Event, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> getKey(Event value) throws Exception {
                                return Tuple2.of(value.getId(), value.getProductionId());
                            }
                        });

        Pattern<Event, Event> pattern = Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new StartCondition("action == 0"))
                .timesOrMore(3)
                .followedBy("end")
                .where(new EndCondition());

        printPatternToJSONString(pattern);

        SingleOutputStreamOperator<String> output = CEP.dynamicPatterns(
                keyedStream,
                new JDBCPeriodicPatternProcessorDiscovererFactory<>(
                        params.get(JDBC_URL_ARG, JDBC_URL_DEFAULT),
                        JDBC_DRIVE,
                        params.get(TABLE_NAME_ARG, TABLE_NAME_DEFAULT),
                        null,
                        Long.parseLong(params.get(JDBC_INTERVAL_MILLIS_ARG,"5000"))),
                TimeBehaviour.ProcessingTime,
                TypeInformation.of(new TypeHint<String>() {})
        );

        output.print();
        env.execute("CEPDemo");
    }

    public static void printPatternToJSONString(Pattern<?, ?> pattern) throws JsonProcessingException {
        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));
    }



}
