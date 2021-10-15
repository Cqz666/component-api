package com.cqz.component.flink.sql.client;

import com.alibaba.fastjson.JSON;
import com.cqz.component.flink.sql.demo.FlinkSQLDemo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


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
        String dql= "select * from tb_socket";
        //注册表
        tEnv.executeSql(ddl);
        //查询
        Table table = tEnv.sqlQuery(dql);

        //table -> dataStream
        DataStream<Tuple2<Boolean, Row>> stream = tEnv.toRetractStream(table, Row.class);

        stream.writeToSocket("127.0.0.1", 6324, new SerializationSchema<Tuple2<Boolean, Row>>() {

            @Override
            public byte[] serialize(Tuple2<Boolean, Row> tuple2) {
                return  tuple2.f1.toString().getBytes();
            }
        });

        stream.print();
        env.execute();

    }

}
