package com.cqz.component.flink.sql.utils;

import java.util.Properties;

public class KafkaUtil {

    public static Properties buildProducerProperties(boolean isByteArraySerializer){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        if (isByteArraySerializer){
            props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        }else {
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }
        props.put("transaction.timeout.ms",5*60*1000);
        return props;
    }

}
