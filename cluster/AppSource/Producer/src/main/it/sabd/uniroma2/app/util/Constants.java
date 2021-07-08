package it.sabd.uniroma2.app.util;

import it.sabd.uniroma2.app.enums.WindowSize;

import java.util.Map;
import java.util.HashMap;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;

//Constants for application and connection configuration
public class Constants {

    //If set to True, the app can run in local, used to debug query without Kafka
    public static boolean MOCK = true;

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
    public static final float MINUTE_DURATION = 10f;

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

    //Tags appendend to the output of the queries to distinguish the output
    public static final String QUERY1_OUTPUT_TAG = "Query1";
    public static final String QUERY2_OUTPUT_TAG = "Query2";
    public static final String QUERY3_OUTPUT_TAG = "Query3";
    //Appended Char to Flink Output, used as delimiter between the Tag of the output and the output itself
    public static final String OUTPUT_DIVIDER = "#";

    //Associate to each Window Size a Long representing the Size, the Sliding factor and
    // a Window offset to align the window
    public static final Map<WindowSize, Tuple3<Time, Time, Time>> WINDOW_MAP = new HashMap<>();
    static {
        WINDOW_MAP.put(WindowSize.WEEKLY, new Tuple3<>(Time.days(7L), Time.days(7L), Time.days(5L)));
        WINDOW_MAP.put(WindowSize.MONTHLY, new Tuple3<>(Time.days(28L), Time.days(28L), Time.days(12L))); //12L
        WINDOW_MAP.put(WindowSize.ONE_HOUR, new Tuple3<>(Time.hours(1L), Time.hours(1L), Time.seconds(0)));
        WINDOW_MAP.put(WindowSize.TWO_HOUR, new Tuple3<>(Time.hours(2L), Time.hours(2L), Time.seconds(0)));
    }

    public static final Long TEST_WINDOW_OFFSET = 428400L;

    public static final float MIN_LAT = 32f;
    public static final float MAX_LAT = 45f;
    public static final float MIN_LON = -6f;
    public static final float MAX_LON = 37f;

    public static final float WEST_SEA_END = 12.275f;

    public static final float CELLS_LAT = 10;
    public static final float CELLS_LON = 40;

    public static final String TIME_SLOT_END = "12:00";

}
