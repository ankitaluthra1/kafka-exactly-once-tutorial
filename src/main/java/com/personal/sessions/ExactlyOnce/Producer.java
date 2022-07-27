package com.personal.sessions.ExactlyOnce;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

public class Producer {

    private KafkaProducer kafkaProducer;

    public Producer(boolean isIdempotent) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        if(isIdempotent) {
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "prod-1");
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
