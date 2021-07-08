package it.sabd.uniroma2.app.util;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class CustomKafkaSerializer implements KafkaSerializationSchema<String> {

    private String topic;

    public CustomKafkaSerializer(String topic){ this.topic = topic; }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, s.getBytes(StandardCharsets.UTF_8));
    }
}
