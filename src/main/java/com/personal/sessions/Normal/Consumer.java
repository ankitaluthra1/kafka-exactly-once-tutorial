package com.personal.sessions.Normal;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    private KafkaConsumer kafkaConsumer;

    public Consumer() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");
        properties.put("enable.auto.commit", true);

        kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(List.of("demo-test-1"));
    }

    public ConsumerRecords<String, String> read() {
        return kafkaConsumer.poll(Duration.ofMillis(10));
    }

    public void close() {
        kafkaConsumer.close();
    }

}
