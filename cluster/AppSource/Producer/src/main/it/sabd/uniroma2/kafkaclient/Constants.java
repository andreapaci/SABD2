package it.sabd.uniroma2.kafkaclient;

//Constants for application and connection configuration
public class Constants {

    //Duration of a Single minute expressed in Microseconds
    //public static final int MINUTES_PER_SECOND = 60 * 24 * 7 * 2;
    public static final float MINUTE_DURATION = 1f;

    //KAFKAHOST and KAFKAPORT used in case "kafka.proprerties" in not avaliable
    public static final String KAFKA_HOSTS = "broker1:9092,broker2:9092";

    //Number of partition for a single topic
    public static final int PARTITION_NUMBER = 2;
    //Replication factor for a single topic
    public static final int REPLICATION_FACTOR = 2;

    public static final String INPUT_TOPIC_NAME = "input_topic";
    public static final String OUTPUT_TOPIC_NAME = "output_topic";
}
