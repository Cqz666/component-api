package com.cqz.component.flink.connector.datagen;

import lombok.Data;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;

public class DataStreamDataGen {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TypeInformation<Integer> typeInfo = TypeInformation.of(Integer.class);
        DataStreamSource<Integer> intGen = env.addSource(new DataGeneratorSource<>(RandomGenerator.intGenerator(1, 1000)),typeInfo);

        TypeInformation<OrderInfo> oITypeInfo = TypeInformation.of(OrderInfo.class);
        DataStreamSource<OrderInfo> orderInfods =
                env.addSource(new DataGeneratorSource<>(new RandomGenerator<OrderInfo>() {
            @Override
            public OrderInfo next() {
                OrderInfo orderInfo = new OrderInfo();
                orderInfo.setId(random.nextInt(1, 100000));
//                orderInfo.setName(random.nextHexString(10));
                orderInfo.setUserId(random.nextLong(1, 100000));
                orderInfo.setTotalAmount(random.nextUniform(1, 1000));
                orderInfo.setCreateTime(System.currentTimeMillis());
                return orderInfo;
            }
        },10L,100L),oITypeInfo);

        TypeInformation<UserInfo> uITypeInfo = TypeInformation.of(UserInfo.class);
        DataStreamSource<UserInfo> userInfods =
                env.addSource(new DataGeneratorSource<>(new SequenceGenerator<UserInfo>(1,100) {
                    final RandomDataGenerator random = new RandomDataGenerator();
                    @Override
            public UserInfo next() {
                UserInfo userInfo = new UserInfo();
                userInfo.setId(valuesToEmit.poll().intValue());
                userInfo.setUserId(valuesToEmit.poll());
                userInfo.setAge(random.nextInt(1,100));
                userInfo.setSex(random.nextInt(0,1));
                return userInfo;
            }
        }),uITypeInfo);

//        intGen.print("------------------------");
        orderInfods.print("orderinfo>>>>>>>>>>>>>>>>");
//        userInfods.print("userinfo>>>");

        env.execute();
    }

    @Data
    public static class OrderInfo {
        private Integer id;
        private String name;
        private Long userId;
        private Double totalAmount;
        private Long createTime;
    }
    @Data
    public static class UserInfo {
        private Integer id;
        private Long userId;
        private Integer age;
        private Integer sex;
    }

}
