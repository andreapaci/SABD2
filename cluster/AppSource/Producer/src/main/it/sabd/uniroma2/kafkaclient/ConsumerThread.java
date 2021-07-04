package it.sabd.uniroma2.kafkaclient;

public class ConsumerThread implements Runnable {

    public void run() {

        System.out.println("Kafka Consumer Thread started.");

        KafkaClient.getInstance().addOutputTopic();


    }

}
