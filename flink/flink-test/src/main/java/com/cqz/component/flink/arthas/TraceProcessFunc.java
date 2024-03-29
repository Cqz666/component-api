package com.cqz.component.flink.arthas;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TraceProcessFunc extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void processElement(Tuple2<String, Long> value,
                               KeyedProcessFunction<String, Tuple2<String, Long>, String>.Context ctx,
                               Collector<String> out) throws Exception {
        a();
        out.collect(value.f0 + "_" + value.f1);
    }

    private void a(){
        sleep(100);
        b();
    }

    private void b(){
        sleep(200);
        c();
    }

    private void c(){
        sleep(3000);
    }





    private void sleep(long mill){
        try {
            Thread.sleep(mill);
        } catch (InterruptedException e) {
        }
    }


}
