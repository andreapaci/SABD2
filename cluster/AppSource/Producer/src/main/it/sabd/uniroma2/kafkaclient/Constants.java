package it.sabd.uniroma2.kafkaclient;

//Constants for application and connection configuration
public class Constants {

    //System enviroment variable name which decides if the application is the
    //Kafka Producer/Consumer or the Flink Jar Topology
    public static final String APP_GOAL_NAME = "APP_GOAL";
    //Goal for Client .jar
    public static final String CLIENT_GOAL = "client";
    //Goal for Flink Job .jar
    public static final String FLINK_GOAL = "flinkjob";
    //Path to application .jar
    public static final String APP_PATH = "./app.jar";

    //Duration of a Single minute expressed in Microseconds
    //public static final int MINUTES_PER_SECOND = 60 * 24 * 7 * 2;
    public static final float MINUTE_DURATION = 1000f;

    //Used for connection to Kafka
    //public static final String KAFKA_HOSTS = "localhost:9093,localhost:9094";
    public static final String KAFKA_HOSTS = "broker1:9092,broker2:9092";

    //Job Manager Host
    //public static final String FLINK_HOSTNAME = "localhost";
    public static final String FLINK_HOSTNAME = "jobmanager";
    public static final int FLINK_PORT = 8081;

    //Number of partition for a single topic
    public static final int PARTITION_NUMBER = 2;
    //Replication factor for a single topic
    public static final int REPLICATION_FACTOR = 2;

    public static final String INPUT_TOPIC_NAME = "input_topic";
    public static final String OUTPUT_TOPIC_NAME = "output_topic";


    public static final float MIN_LAT = 32f;
    public static final float MAX_LAT = 45f;
    public static final float MIN_LON = -6f;
    public static final float MAX_LON = 37f;

    public static final float CELLS_LAT = 10;
    public static final float CELLS_LON = 40;

}
