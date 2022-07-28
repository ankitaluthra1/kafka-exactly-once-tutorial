package com.personal.sessions.Normal;

public class UpstreamProducer {
    public static void main(String[] args) {
        Producer producer = new Producer();

        try {
            for (int i = 0; i < 2000; i++) {
                try {
                    producer.produce(Consumer.UPSTREAM_TOPIC, "Source message " + i);
                    System.out.println("Producing message");
                    Thread.sleep(500);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            producer.close();
        }

    }


}
