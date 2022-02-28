package com.cqz.component.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<String> elments ;
        if (args.length==0){
            elments = new ArrayList<>(Collections.singletonList("没有参数输入"));
        }else {
            elments = new ArrayList<>(Arrays.asList(args));
        }

        DataStream<String> source = env.fromCollection(elments);
        source.print("输入的参数========>");

        env.execute();
    }
}
