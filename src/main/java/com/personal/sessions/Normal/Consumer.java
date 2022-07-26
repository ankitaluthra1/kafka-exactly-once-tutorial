package com.personal.sessions.Normal;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    private KafkaConsumer kafkaConsumer;

    public Consumer(String topic) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-3");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(List.of(topic));
    }

    public ConsumerRecords<String, String> read() {
        return kafkaConsumer.poll(Duration.ofMillis(100));
    }

    public void commitSync() {
        kafkaConsumer.commitSync();
    }

    public void close() {
        kafkaConsumer.close();
    }

}
