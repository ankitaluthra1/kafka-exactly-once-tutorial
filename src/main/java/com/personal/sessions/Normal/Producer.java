package com.personal.sessions.Normal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private KafkaProducer kafkaProducer;

    public Producer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer(properties);
    }

    public void produce(String message) {
        kafkaProducer.send(new ProducerRecord("sink-test-1", message));
    }

    public void close(){
        kafkaProducer.close();
    }

}
