package com.cqz.component.flink.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public  class MockSource implements SourceFunction<Tuple2<String, Long>> {

    private volatile boolean isCancel;
    private static final String[] name = {"A","B","C","D"};

    @Override
    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
        Random random = new Random();
        while (!this.isCancel) {
            int i = random.nextInt(4);
            long l = random.nextInt(100000);
            sourceContext.collect(Tuple2.of(name[i], l));
            Thread.sleep(10L);
        }
    }
    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
