package com.personal.sessions.Normal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Application {

    public static void main(String[] args) {
        Consumer consumer = new Consumer(Consumer.UPSTREAM_TOPIC);
        Producer producer = new Producer();
        try {
            while (true) {
                    ConsumerRecords<String, String> records = consumer.read();

                    for (ConsumerRecord record : records) {
                        System.out.println(String.format("Read - %s, from partition: %s", record.value(), record.partition()));
                        Thread.sleep(1000);
                            producer.produce(Consumer.DOWNSTREAM_TOPIC,"Processed record " + record.value());
                    }
                    if(!records.isEmpty()) {
                        System.out.println("Committed current poll");
                        consumer.commitSync();
                    }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
            producer.close();
        }

    }


}
