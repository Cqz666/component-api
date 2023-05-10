package com.cqz.component.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaComsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = buildConsumerProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("gamebox_event_origin"));
        int cnt=0;
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()){
                Thread.sleep(1000);
                System.out.println("没有消费到数据");
                cnt++;
            }
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                System.out.println(value);
                cnt++;
            }
            if (cnt>5){
                System.out.println("退出程序.");
                consumer.close();
                return;
            }
        }
    }


    private static Properties buildConsumerProperties(){
        Properties props = new Properties();
        //props.put("bootstrap.servers", "10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092");
//        props.put("bootstrap.servers", "10.0.0.196:9092,10.0.0.197:9092,10.0.0.198:9092");
        props.put("bootstrap.servers", "172.21.0.3:9092");
        props.put("group.id","mytest-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("max.poll.records","5000");
        return props;
    }

}
