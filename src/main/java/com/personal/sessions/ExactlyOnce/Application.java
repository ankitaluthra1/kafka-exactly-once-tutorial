package com.personal.sessions.ExactlyOnce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Application {

    public static void main(String[] args) {
        Consumer consumer = new Consumer(Consumer.UPSTREAM_TOPIC);
        Producer producer = new Producer(true);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.read();
                if(!records.isEmpty()){
                    producer.beginTransaction();
                }
                for (ConsumerRecord record : records) {
                    System.out.println(String.format("Read - %s, from partition: %s", record.value(), record.partition()));
                    Thread.sleep(500);
                    producer.produce(Consumer.DOWNSTREAM_TOPIC, "Processed record " + record.value());
                }
                if(!records.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = getOffset(records);
                    producer.sendOffsetsToTransaction(topicPartitionOffsetAndMetadataMap);
                    producer.commitTransaction();
                    System.out.println("Committed current poll");
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            producer.abortTransaction();
        } finally {
            consumer.close();
            producer.close();
        }

    }

    private static Map<TopicPartition, OffsetAndMetadata> getOffset(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }

        return offsetsToCommit;
    }


}
