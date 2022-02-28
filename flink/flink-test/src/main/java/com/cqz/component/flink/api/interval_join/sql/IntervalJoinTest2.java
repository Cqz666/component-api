package com.cqz.component.flink.api.interval_join.sql;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class IntervalJoinTest2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        KeyedStream<Tuple3, Tuple> show = env.fromElements(
                new Tuple3("key_a", "show", 123L),
                new Tuple3("key_b", "show", 124L),
                new Tuple3("key_c", "show", 125L),
                new Tuple3("key_d", "show", 126L),
                new Tuple3("key_e", "show", 127L),
                new Tuple3("key_show", "show", 128L)
        ).keyBy("f0");

        KeyedStream<Tuple3, Tuple> click = env.fromElements(
                new Tuple3("key_a", "click", 126L),
                new Tuple3("key_b", "click", 127L),
                new Tuple3("key_c", "click", 128L),
                new Tuple3("key_d", "click", 129L),
                new Tuple3("key_e", "click", 130L),
                new Tuple3("key_click", "click", 132L)
        ).keyBy("f0");

        Schema schema = Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("ts", "TO_TIMESTAMP(FROM_UNIXTIME(f2 , 'yyyy-MM-dd HH:mm:ss'))")
//                .watermark("ts", "ts - INTERVAL '5' SECOND")
                .build();
        tEnv.createTemporaryView("dclick", click, schema);
        tEnv.createTemporaryView("dshow", show, schema);
        Table table = tEnv.sqlQuery("select dshow.f0,dshow.f1,dshow.f2,\n" +
                "dclick.f0,dclick.f1,dclick.f2\n" +
                "from\n" +
                "--dshow right outer join dclick\n" +
                "dclick left outer join dshow\n" +
                "on dshow.f0 = dclick.f0\n" +
                "and dclick.ts BETWEEN dshow.ts - INTERVAL '10' MINUTE AND dshow.ts + INTERVAL '10' MINUTE");
        table.execute().print();

    }
}
