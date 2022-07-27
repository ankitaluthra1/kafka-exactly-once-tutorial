package com.personal.sessions.ExactlyOnce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class DownstreamConsumer {

    public static void main(String[] args) {
        Consumer consumer = new Consumer("sink-test-17");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.read();

                for (ConsumerRecord record : records) {
                    System.out.println(record.value());
                }
                if (!records.isEmpty()) {
                    System.out.println("Committed current poll");
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
        }

    }

}
