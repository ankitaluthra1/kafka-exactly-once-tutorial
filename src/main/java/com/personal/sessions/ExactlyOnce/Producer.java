package com.personal.sessions.ExactlyOnce;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

public class Producer {

    private KafkaProducer kafkaProducer;

    public Producer(boolean isIdempotent) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        if(isIdempotent) {
            properties.put("enable.idempotence", "true");
            properties.put("transactional.id", "prod-1");
        }
        kafkaProducer = new KafkaProducer(properties);
        if(isIdempotent)
            kafkaProducer.initTransactions();
    }

    public void produce(String topic, String message) {
        kafkaProducer.send(new ProducerRecord(topic, message));
    }

    public void close(){
        kafkaProducer.close();
    }

    public void beginTransaction(){
        kafkaProducer.beginTransaction();
    }

    public void commitTransaction(){
        kafkaProducer.commitTransaction();
    }

    public void abortTransaction() {
        kafkaProducer.abortTransaction();
    }

    public void initTransaction() {
        kafkaProducer.initTransactions();
    }

    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap) {
        kafkaProducer.sendOffsetsToTransaction(topicPartitionOffsetAndMetadataMap, Consumer.CONSUMER_GROUP_ID);
    }
}
