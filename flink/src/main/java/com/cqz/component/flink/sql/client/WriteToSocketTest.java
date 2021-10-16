package com.cqz.component.flink.sql.client;

import com.cqz.component.flink.sql.demo.FlinkSQLDemo;
import com.cqz.component.flink.sql.kafka.KafkaSerializerSchema;
import com.cqz.component.flink.sql.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class WriteToSocketTest {

    private static String DATA_PATH = FlinkSQLDemo.class.getClassLoader().getResource("data.txt").toString();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String ddl = "CREATE TABLE tb_socket (id int,name String)" +
                "with('connector' = 'filesystem'," +
                "'path' = '"+DATA_PATH+"'," +
                "'format' = 'csv'" +
                ")";

        String dql= "select count(*) as cnt,id from tb_socket group by id";
        //注册表
        tEnv.executeSql(ddl);
        //查询
        Table table = tEnv.sqlQuery(dql);
//        TableResult execute = table.execute();

        //table -> dataStream
        DataStream<Tuple2<Boolean, Row>> stream = tEnv.toRetractStream(table, Row.class);

//        stream.writeToSocket("127.0.0.1", 6324, new SerializationSchema<Tuple2<Boolean, Row>>() {
//            @Override
//            public byte[] serialize(Tuple2<Boolean, Row> tuple2) {
//                return  tuple2.f1.toString().getBytes();
//            }
//        });

        stream.addSink(new FlinkKafkaProducer<Tuple2<Boolean, Row>>(
                "test",
                new KafkaSerializerSchema("test"),
                KafkaUtil.buildProducerProperties(true),
                FlinkKafkaProducer.Semantic.NONE));

        stream.print();
        env.execute();

    }

}
