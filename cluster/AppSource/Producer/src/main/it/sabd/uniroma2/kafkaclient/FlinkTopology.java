package it.sabd.uniroma2.kafkaclient;




import it.sabd.uniroma2.kafkaclient.entity.NavalData;
import it.sabd.uniroma2.kafkaclient.util.CustomKafkaSerializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class FlinkTopology {

    private StreamExecutionEnvironment executionEnvironment;
    private Properties properties;

    public FlinkTopology(){

        properties = new Properties();

        properties.put("bootstrap.servers", Constants.KAFKA_HOSTS);
        properties.put("group.id", "flink");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");

        executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    }

    public void defineTopology(){

        DataStream<NavalData> dataStream = registerSource();

        dataStream = preproccessing(dataStream);


        //addSink(dataStream);

        try {
            executionEnvironment.execute("Query");
        } catch (Exception e) {
            System.out.println("An error occurred executing the Flink Job.");
            e.printStackTrace();
        }

    }

    private DataStream<NavalData> registerSource(){

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(Constants.INPUT_TOPIC_NAME, new SimpleStringSchema(), properties);
        kafkaSource.setStartFromEarliest();

        DataStream<String> dataStream = executionEnvironment.addSource(kafkaSource);

        return dataStream.map((MapFunction<String, NavalData>) s -> {

                    SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm");

                    String[] values = s.split(",");

                    Date formattedDate = null;
                    try{ formattedDate = df.parse(values[0]); }
                    catch (Exception e) { System.out.println("Could not parse date: " + values[0]); e.printStackTrace(); }

                    return new NavalData(formattedDate, values[1], values[2], Float.parseFloat(values[3]), Float.parseFloat(values[4]));

                });


    }

    private DataStream<NavalData> preproccessing(DataStream<NavalData> dataStream){

        dataStream = filterStream(dataStream);

        dataStream = addCells(dataStream);

        dataStream = setSea(dataStream);


        return dataStream;


    }

    private DataStream<NavalData> filterStream(DataStream<NavalData> dataStream){

        dataStream.filter((FilterFunction<NavalData>) nav ->
                nav.getLat() >= Constants.MIN_LAT &&
                nav.getLat() <= Constants.MAX_LAT &&
                nav.getLon() >= Constants.MIN_LON &&
                nav.getLon() <= Constants.MAX_LON);

        return dataStream;
    }

    private DataStream<NavalData> addCells(DataStream<NavalData> dataStream){

        
        return dataStream;
    }

    private DataStream<NavalData> setSea(DataStream<NavalData> dataStream){

        return dataStream;
    }

    private void addSink(DataStream<> datastream){

        datastream.print();
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(Constants.OUTPUT_TOPIC_NAME, new SimpleStringSchema(), properties);

        datastream.addSink(kafkaSink);

    }



}
