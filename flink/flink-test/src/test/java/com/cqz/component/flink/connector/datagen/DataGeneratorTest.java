package com.cqz.component.flink.connector.datagen;

import com.cqz.component.flink.connector.datagen.pojo.DummyPojo;
import com.cqz.component.flink.connector.datagen.pojo.OrderInfo;
import com.cqz.component.flink.connector.datagen.pojo.UserInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class DataGeneratorTest {
    private StreamExecutionEnvironment env;
    private DataGenerator dataGenerator;

    @Before
    public void setUp() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        dataGenerator = new DataGenerator<>(env);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void toDataStream() throws Exception {
        DataGenDescriptor userDescriptor = DataGenDescriptor.forPojo(UserInfo.class)
                .rowsPerSecond(1)
                .numberOfRows(10000)
                .build();
        DataGenDescriptor orderDescriptor = DataGenDescriptor.forPojo(OrderInfo.class)
                .rowsPerSecond(2)
                .numberOfRows(10000)
                .build();
        DataGenDescriptor dummyDescriptor = DataGenDescriptor.forPojo(DummyPojo.class).build();

        DataStream<UserInfo> userStream = dataGenerator.toDataStream(userDescriptor);
        DataStream<OrderInfo> orderStream = dataGenerator.toDataStream(orderDescriptor);
        DataStream<DummyPojo> dummyPojoDataStream = dataGenerator.toDataStream(dummyDescriptor);
        Arrays.stream(dataGenerator.getTableEnv().listTables()).forEach(System.out::println);
        userStream.print();
        orderStream.print();
//        dummyPojoDataStream.print();
        env.execute();
    }

    @Test
    public void getSchema() {
    }

    @Test
    public void getConnectorOptions() {
//        System.out.println(userdataGenerator.getConnectorOptions().toString());
    }
}