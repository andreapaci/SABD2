package it.sabd.uniroma2.kafkaclient;

import java.util.List;


public class Main {

    public static void main(String[] args) {

        //TODO: download dei dati da repo

        List<String[]> dataset = loadDataset(false, false);

        kafkaRoutines(dataset);

    }


    private static List<String[]> loadDataset(boolean printOutput, boolean saveDataset){


        CSVParser csvParser = new CSVParser("dataset.csv");
        List<String[]> dataset = csvParser.parseAndSort(saveDataset);

        if(printOutput) {
            for (String[] value : dataset) {
                System.out.print(value[0] + ": ");
                for (int i = 1; i < value.length; i++) {
                    System.out.print(value[i] + ";; ");
                }
                System.out.println("");
            }

            System.out.println("Dataset rows: " + dataset.size());
        }



        return dataset;


    }

    private static void kafkaRoutines(List<String[]> dataset){

        System.out.println("Starting Kafka Threads...");
        (new Thread(new ProducerThread(dataset))).start();
        (new Thread(new ConsumerThread())).start();



    }

}
