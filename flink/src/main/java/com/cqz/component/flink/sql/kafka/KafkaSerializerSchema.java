package com.cqz.component.flink.sql.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Set;

public class KafkaSerializerSchema implements KafkaSerializationSchema<Tuple2<Boolean, Row>> {

    private String topic;

    public KafkaSerializerSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<Boolean, Row> element, @Nullable Long timestamp) {

        Boolean f0 = element.f0;
        Row f1 = element.f1;
        String result = f0+","+f1.toString();

        return new ProducerRecord<>(topic, result.getBytes(StandardCharsets.UTF_8));
    }


}
