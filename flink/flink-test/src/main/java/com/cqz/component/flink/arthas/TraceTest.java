package com.cqz.component.flink.arthas;

import com.cqz.component.flink.utils.MockSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TraceTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = Configuration.fromMap(parameterTool.toMap());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.disableOperatorChaining();
        DataStream<Tuple2<String, Long>> source = env.addSource(new MockSource());
        SingleOutputStreamOperator<String> streamOperator = source.keyBy(t -> t.f0)
                .process(new TraceProcessFunc());
        streamOperator.print();
        env.execute();
    }
}
