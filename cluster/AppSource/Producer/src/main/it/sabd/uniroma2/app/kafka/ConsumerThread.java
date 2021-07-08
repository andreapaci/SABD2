package it.sabd.uniroma2.app.kafka;

public class ConsumerThread implements Runnable {

    public void run() {

        System.out.println("Kafka Consumer Thread started.");

        KafkaClient.getInstance().addOutputTopic();


    }

}
