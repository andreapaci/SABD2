package it.sabd.uniroma2.app.kafka;

import it.sabd.uniroma2.app.util.CSVWriter;

import java.util.ArrayList;

public class ConsumerThread implements Runnable {

    public void run() {

        System.out.println("Kafka Consumer Thread started.");

        KafkaClient.getInstance().addOutputTopic();

        CSVWriter csvWriter = new CSVWriter();

        while(true) {
            ArrayList<String> output = KafkaClient.getInstance().readMessage();
            csvWriter.write(output);
        }


    }

}
