package it.sabd.uniroma2.app.flink;




import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.enums.Seas;
import it.sabd.uniroma2.app.enums.TimeSlot;
import it.sabd.uniroma2.app.enums.WindowSize;
import it.sabd.uniroma2.app.queries.query1.Query1;
import it.sabd.uniroma2.app.queries.query2.Query2;
import it.sabd.uniroma2.app.util.Constants;
import it.sabd.uniroma2.app.util.Utils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

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

        Query1 query1Month = new Query1(WindowSize.MONTHLY);
        query1Month.defineQuery(dataStream);

        Query1 query1Week = new Query1(WindowSize.WEEKLY);
        query1Week.defineQuery(dataStream);

        Query2 query2Month = new Query2(WindowSize.MONTHLY);
        query2Month.defineQuery(dataStream);

        Query2 query2Week = new Query2(WindowSize.WEEKLY);
        query2Week.defineQuery(dataStream);

        //Query2 query2 = new Query2(WindowSize.WEEKLY);
        //query2.defineQuery(dataStream);


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
                        .windowAll(SlidingEventTimeWindows.of(Time.days(7L), Time.days(7L), Time.seconds(Constants.TEST_WINDOW_OFFSET)))
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
            /*DataStream<NavalData> shipsRoutes = executionEnvironment.fromElements(
                    new NavalData(Utils.formatStringToDate("2015/03/10 12:15"), "0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a", 99,
                            (float) 10.56034, (float) 35.8109, "0xc35c9_10-03-15 12:xx - 10-03-15 13:26"),

                    new NavalData(Utils.formatStringToDate("2015/03/11 12:13"), "0xc35c9ebbf48cbb5857a868ce441824d0bpippo", 66,
                            (float) 11.56034, (float) 35.8109, "0xc35c9_10-03-15 12:xx - 10-03-15 13:26"),

                    new NavalData(Utils.formatStringToDate("2015/03/11 12:15"), "0xc35c9ebbf48cbb5857a868ce441824d0b2topolino", 33,
                            (float) -5.354218, (float) 35.8109, "0xc35c9_10-03-15 12:xx - 10-03-15 13:26"),

                    new NavalData(Utils.formatStringToDate("2015/03/12 12:15"), "0xc35c9ebbf48cbb5857a868ce441824d0b2pluto", 70,
                            (float) 11.56034, (float) 35.8109, "0xc35c9_10-03-15 12:xx - 10-03-15 13:26"),

                    new NavalData(Utils.formatStringToDate("2015/03/13 12:15"), "0xc35c9ebbf48cbb5857a868ce441824d0bpaperino", 35,
                            (float) -5.354218, (float) 35.8109, "0xc35c9_10-03-15 12:xx - 10-03-15 13:26"),

                    new NavalData(Utils.formatStringToDate("2015/03/14 13:51"), "0x6cafd52f3e1a09950e6e889daabf81bafbe7a40a", 35,
                            (float) -5.374995, (float) 36.04406, "0x6cafd_14-03-15 13:xx - 18-04-15 16:29"),

                    new NavalData(Utils.formatStringToDate("2015/03/18 06:55"), "0x6d6794f3186f584637721a1e1789fd2e71c28195", 83,
                            (float) -5.354218, (float) 35.92629, "0x6d679_17-03-15 7:xx - 20-03-15 18:18"),

                    new NavalData(Utils.formatStringToDate("2015/03/18 06:57"), "0xb633ed54f31994072009a034d63d3b65e471fba9", 30,
                            (float) 14.52148, (float) 35.8989, "0xb633e_18-03-15 6:xx - 18-03-15 8:24"),

                    new NavalData(Utils.formatStringToDate("2015/03/18 06:58"), "0x6d6794f3186f584637721a1e1789fd2e71c28195", 83,
                            (float) -5.354218, (float) 35.92628, "0x6d679_17-03-15 7:xx - 20-03-15 18:18"),

                    new NavalData(Utils.formatStringToDate("2015/03/18 06:59"), "0xb633ed54f31994072009a034d63d3b65e4culo", 30,
                            (float) 8.52536, (float) 35.90051, "0xb633e_18-03-15 6:xx - 18-03-15 8:24"),

                    new NavalData(Utils.formatStringToDate("2020/03/18 06:59"), "0xb633ed54f31994072009a034d63d3b65e4cazzo", 30,
                            (float) 8.52536, (float) 35.90051, "0xb633e_18-03-15 6:xx - 18-03-15 8:24")

            );
            return shipsRoutes; */

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
            return new NavalData(formattedDate, values[1], values[2], Float.parseFloat(values[4]), Float.parseFloat(values[5]), values[10]);
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
                .<NavalData>forBoundedOutOfOrderness(Duration.ofMinutes(5L));
        //TODO: Mettere giusto watermark
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
