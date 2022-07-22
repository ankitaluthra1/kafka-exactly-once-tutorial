package com.personal.sessions.Normal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Main1 {

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        Producer producer = new Producer();
        try {
            while (true) {
                    ConsumerRecords<String, String> records = consumer.read();

                    for (ConsumerRecord record : records) {
                        System.out.println(String.format("Read - %s, from partition: %s", record.value(), record.partition()));
                        System.out.println("Processed");
                        Thread.sleep(1000);
                            producer.produce("Processed record " + record.value());

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
