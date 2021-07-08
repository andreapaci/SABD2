package it.sabd.uniroma2.app.kafka;

import it.sabd.uniroma2.app.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

//Kafka Producer Thread
public class ProducerThread implements Runnable {

    private List<String[]> dataset;
    private SimpleDateFormat dateFormatter;

    public ProducerThread(List<String[]> dataset){
        this.dataset = dataset;

        dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm");
    }

    public void run() {

        System.out.println("Kafka Producer Thread started.");


        KafkaClient.getInstance().addInputTopic();

        Date previousDate = formatDate(dataset.get(0)[0]);
        long index = 0;

        while(!dataset.isEmpty()){

            Date actualDate = formatDate(dataset.get(0)[0]);

            if(actualDate.before(previousDate)) {
                System.out.println("Error! Dates not in order.");
                actualDate = previousDate;
            }

            long diff = actualDate.getTime() - previousDate.getTime();
            long waitTime = (long) (((float) diff / (1000f * 60f)) * Constants.MINUTE_DURATION);


            try { TimeUnit.MICROSECONDS.sleep(waitTime); }
            catch(Exception e) { System.out.println("Could not wait " + waitTime + " microseconds."); }

            KafkaClient.getInstance().sendMessage(actualDate.getTime(), arrayToCommaString(dataset.get(0)));

            if(index % 500 == 0)
                System.out.println("Message " + Long.toString(index) + " sending scheduled [" + dataset.get(0)[0] + "]");

            dataset.remove(0);

            previousDate = actualDate;

            index++;

        }

        System.out.println("All messages sent.");

        KafkaClient.getInstance().killProducer();

        System.out.println("Killing Kafka Producer Thread...");
    }


    private Date formatDate(String date){

        Date formDate = null;

        try {
            formDate = dateFormatter.parse(date);
        } catch (Exception e) {
            System.out.println("Date not parseable: " + date);
            e.printStackTrace();
        }

        return formDate;

    }

    private String arrayToCommaString(String[] array){

        String value = array[0];

        for(int i = 1; i < array.length; i++){
            value += "," + array[i];
        }

        return value;
    }





}
