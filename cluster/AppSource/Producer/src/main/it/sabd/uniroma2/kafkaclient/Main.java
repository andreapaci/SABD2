package it.sabd.uniroma2.kafkaclient;

import java.util.List;
import java.util.concurrent.TimeUnit;


public class Main {

    public static void main(String[] args) {

        //System variable to decide jar goal (Kafka Client or Flink Job)
        if(args.length == 1) if(args[0].equals("scan")) flinkJob();

        String appGoal = System.getenv(Constants.APP_GOAL_NAME);

        if(appGoal == null){
            System.out.println("No system variable \"" + Constants.APP_GOAL_NAME + "\"\nExiting... ");
            System.exit(-1);
        }
        switch(appGoal){
            case Constants.CLIENT_GOAL:
                client();
                break;
            case Constants.FLINK_GOAL:
                flinkJob();
                break;
            default:
                System.out.println("Goal incorrect. Exiting...");
                break;

        }


    }

    private static void client(){
        //TODO: download dei dati da repo

        try{TimeUnit.SECONDS.sleep(10l);} catch(Exception e) { e.printStackTrace();}

        System.out.println("Sleep for other 10 seconds...");

        try{TimeUnit.SECONDS.sleep(10l);} catch(Exception e) { e.printStackTrace();}

        System.out.println("Done.");

        System.out.println(System.getenv(Constants.APP_GOAL_NAME));

        submitTopology();

        List<String[]> dataset = loadDataset(false, false);

        kafkaRoutines(dataset);
    }


    private static void flinkJob(){
       FlinkTopology flinkTopology = new FlinkTopology();

       flinkTopology.defineTopology();
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

    private static void submitTopology(){

        System.out.println("Instancing Flink handler");

        FlinkHandler flinkHandler = new FlinkHandler();

        System.out.println("Submitting job");

        flinkHandler.submitJob();

    }

}
