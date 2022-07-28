package com.personal.sessions.ExactlyOnce;

public class UpstreamProducer {
    public static void main(String[] args) {
        Producer producer = new Producer(false);

        try {
            for (int i = 0; i < 100; i++) {
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
