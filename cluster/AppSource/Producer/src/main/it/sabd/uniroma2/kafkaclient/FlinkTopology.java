package it.sabd.uniroma2.kafkaclient;




import it.sabd.uniroma2.kafkaclient.entity.NavalData;
import it.sabd.uniroma2.kafkaclient.enums.Seas;
import it.sabd.uniroma2.kafkaclient.enums.TimeSlot;
import it.sabd.uniroma2.kafkaclient.queries.Query2Aggregator;
import it.sabd.uniroma2.kafkaclient.queries.Query2Result;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;


public class FlinkTopology {

    private StreamExecutionEnvironment executionEnvironment;
    private Properties properties;

    public FlinkTopology(){

        properties = new Properties();

        properties.put("bootstrap.servers", Constants.KAFKA_HOSTS);
        properties.put("group.id", "flink");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");

        if(Constants.MOCK) {
            executionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        } else {
            executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        }

    }


    public void defineTopology(){

        DataStream<NavalData> dataStream = registerSource();

        dataStream = preproccessing(dataStream);

        //dataStream.print();

        //testStream(dataStream);



        //dataStream = addSink(dataStream);

        // TODO: differenza tra datastream.execute e env.execute

        try {
            executionEnvironment.execute("Query");
        } catch (Exception e) {
            System.out.println("An error occurred executing the Flink Job.");
            e.printStackTrace();
        }

    }

    private void testStream(DataStream<NavalData> dataStream){

        SingleOutputStreamOperator<String> outputStreamOperator =
                dataStream
                        .map(new MapFunction<NavalData, String>() {
                            @Override
                            public String map(NavalData navalData) throws Exception {
                                return navalData.getFormattedTs();
                            }
                        })
                        .windowAll(SlidingEventTimeWindows.of(Time.days(7L), Time.days(7L), Time.seconds(Constants.WINDOW_OFFSET)))
                        .reduce(new ReduceFunction<String>() {
                            @Override
                            public String reduce(String s, String t1) throws Exception {
                                return s + "---*---" + t1;
                            }
                        });

        outputStreamOperator.print();
    }







    private DataStream<NavalData> registerSource(){

        DataStream<String> dataStream = null;

        if(Constants.MOCK){

            dataStream = executionEnvironment.readTextFile("mock_dataset.txt");


        } else {
            FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(Constants.INPUT_TOPIC_NAME, new SimpleStringSchema(), properties);
            kafkaSource.setStartFromEarliest();

            dataStream = executionEnvironment.addSource(kafkaSource);
        }
        return dataStream.map((MapFunction<String, NavalData>) s -> {

            SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm");

            String[] values = s.split(",");

            Date formattedDate = null;
            try {
                formattedDate = df.parse(values[0]);
            } catch (Exception e) {
                System.out.println("Could not parse date: " + values[0]);
                e.printStackTrace();
            }

            //TODO: controlla il dato inviato da Kafka e vedi se l'ordine è corretto
            return new NavalData(formattedDate, values[1], values[2], Float.parseFloat(values[4]), Float.parseFloat(values[5]));
        });



    }

    private DataStream<NavalData> preproccessing(DataStream<NavalData> dataStream){

        dataStream = filterStream(dataStream);

        dataStream = addCells(dataStream);

        dataStream = setSea(dataStream);

        dataStream = setTimeSlot(dataStream);

        dataStream = assignTimestamp(dataStream);

        return dataStream;


    }

    private DataStream<NavalData> filterStream(DataStream<NavalData> dataStream){

        dataStream = dataStream.filter((FilterFunction<NavalData>) nav ->
                    nav.getLat() >= Constants.MIN_LAT &&
                    nav.getLat() <= Constants.MAX_LAT &&
                    nav.getLon() >= Constants.MIN_LON &&
                    nav.getLon() <= Constants.MAX_LON);


        return dataStream;
    }

    private DataStream<NavalData> addCells(DataStream<NavalData> dataStream){


         dataStream = dataStream.map((MapFunction<NavalData, NavalData>) navalData -> {

             float lon = navalData.getLon();
             float lat = navalData.getLat();
             String lat_component = null;
             String lon_component = null;

             //TODO: calcola bene sta roba
             if(lat >= 32 && lat < 33.3){
                 lat_component = "A";
             } else if(lat >= 33.3 && lat < 34.6){
                 lat_component = "B";
             } else if(lat >= 34.6 && lat < 35.9){
                 lat_component = "C";
             } else if(lat >= 35.9 && lat < 37.2){
                 lat_component = "D";
             } else if(lat >= 37.2 && lat < 38.5){
                 lat_component = "E";
             } else if(lat >= 38.5 && lat < 39.8){
                 lat_component = "F";
             } else if(lat >= 39.8 && lat < 41.1){
                 lat_component = "G";
             } else if(lat >= 41.1 && lat < 42.4){
                 lat_component = "H";
             } else if(lat >= 42.4 && lat < 43.7){
                 lat_component = "I";
             } else if(lat >= 43.7 && lat < 45){
                 lat_component = "J";
             }

             float lon_start = Constants.MIN_LON;
             float lon_end = lon_start + 1.075f;

             for(Integer i = 1; i < 41; i++){
                 if(lon >= lon_start && lon < lon_end){
                     lon_component = i.toString();
                     break;
                 }

                 lon_start = lon_end;
                 lon_end = lon_end + 1.075f;
             }


             if (lat_component == null)
                 throw new Exception("Error in latitude values inserted: " + navalData.getLat());

             if (lon_component == null)
                 throw new Exception("Error in longitude values inserted: " + navalData.getLon());

             String cell = lat_component + lon_component;

             navalData.setCell(cell);

             return navalData;
         });

        return dataStream;
    }


    private DataStream<NavalData> setSea(DataStream<NavalData> dataStream){

        dataStream = dataStream.map((MapFunction<NavalData, NavalData>) navalData -> {

            float lon = navalData.getLon();
            //TODO: fai bene divisione mare
            Seas sea;
            if(lon <= Constants.WEST_SEA_END) sea = Seas.WESTERN_MEDITERANEAN_SEA;
            else sea = Seas.EASTERN_MEDITERANEAN_SEA;

            navalData.setSea(sea);

            return navalData;
        });

        return dataStream;
    }

    private DataStream<NavalData> setTimeSlot(DataStream<NavalData> dataStream){

        dataStream = dataStream.map((MapFunction<NavalData, NavalData>) navalData -> {

            SimpleDateFormat hourFormat = new SimpleDateFormat("HH:mm");

            String date = hourFormat.format(navalData.getTs());

            TimeSlot slot;

            if(date.compareTo(Constants.TIME_SLOT_END) < 0) slot = TimeSlot.BEFORE_NOON;
            else slot = TimeSlot.AFTER_NOON;

            navalData.setTimeSlot(slot);

            return navalData;
        });

        return dataStream;
    }

    private DataStream<NavalData> assignTimestamp(DataStream<NavalData> dataStream){

        //TODO: imposa bene windows
        WatermarkStrategy<NavalData> watermarkStrategy = WatermarkStrategy
                .<NavalData>forBoundedOutOfOrderness(Duration.ofDays(1));
        if(Constants.MOCK)
                watermarkStrategy = watermarkStrategy.withTimestampAssigner((event, timestamp) -> event.getTs().getTime());

        dataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);

        return dataStream;
    }

    private void addSink(DataStream<String> datastream){

        datastream.print();
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(Constants.OUTPUT_TOPIC_NAME, new SimpleStringSchema(), properties);

        datastream.addSink(kafkaSink);

    }



}
