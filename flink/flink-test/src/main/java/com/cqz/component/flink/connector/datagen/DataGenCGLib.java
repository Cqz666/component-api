package com.cqz.component.flink.connector.datagen;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataGenCGLib {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final Map<String, Class< ? >> properties = new LinkedHashMap<>();
        properties.put("id", Integer.class);
        properties.put("userId", Long.class);
        properties.put("age", Integer.class);
        properties.put("sex", Integer.class);

        final Class<?> userInfoClass = CGLibUtil.createBeanClass("UserInfo", properties);
        TypeInformation<?> uITypeInfo = TypeInformation.of(userInfoClass);
        DataStreamSource ds = env.addSource(new DataGeneratorSource<>(new RandomGenerator() {
            @Override
            public Object next() {
                Object obj;
                try {
                    obj = userInfoClass.newInstance();
                    userInfoClass.getMethod("setId", Integer.class).invoke(obj, random.nextInt(1, 10000));
                    userInfoClass.getMethod("setUserId", Long.class).invoke(obj, random.nextLong(1, 10000));
                    userInfoClass.getMethod("setAge", Integer.class).invoke(obj, random.nextInt(1, 100));
                    userInfoClass.getMethod("setSex", Integer.class).invoke(obj, random.nextInt(0, 1));

                } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                         InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                return obj;
            }
        },10L,100L), uITypeInfo);

        SingleOutputStreamOperator getId = ds.map(new MapFunction<Object,String>() {
            @Override
            public String map(Object value) throws Exception {
                Map<String,Object> targetMap = new LinkedHashMap<>();
                for (String prop : properties.keySet()) {
                    targetMap.put(prop, userInfoClass.getMethod("get"+StringUtils.capitalize(prop)).invoke(value));
                }
                return JSONObject.toJSONString(targetMap);
            }
        },uITypeInfo);

        getId.print();

        env.execute();

    }


}
