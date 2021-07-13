package it.sabd.uniroma2.app.flink;



import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.enums.Seas;
import it.sabd.uniroma2.app.enums.TimeSlot;
import it.sabd.uniroma2.app.enums.WindowSize;
import it.sabd.uniroma2.app.queries.query1.Query1;
import it.sabd.uniroma2.app.queries.query2.Query2;
import it.sabd.uniroma2.app.queries.query3.Query3;
import it.sabd.uniroma2.app.util.Constants;
import it.sabd.uniroma2.app.util.PreprocessingLatencyTracker;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.text.SimpleDateFormat;
import java.util.*;


public class FlinkTopology {

    private StreamExecutionEnvironment executionEnvironment;
    private Properties properties;

    public FlinkTopology(){

        properties = new Properties();

        properties.put("bootstrap.servers", Constants.KAFKA_HOSTS);
        properties.put("group.id", "flink");


        if(Constants.MOCK) {
            executionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
            executionEnvironment.setParallelism(1);
        } else {
            executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        }

    }

    public void defineTopology(){

        DataStream<NavalData> dataStream = registerSource();

        dataStream = preproccessing(dataStream);

        if(Constants.PRINT_FLINK_OUTPUT) dataStream.print();

        List<SingleOutputStreamOperator<String>> sinkableStream = new ArrayList<>();

        Query1 query1Month = new Query1(WindowSize.MONTHLY);
        sinkableStream.add(query1Month.defineQuery(dataStream));

        Query1 query1Week = new Query1(WindowSize.WEEKLY);
        sinkableStream.add(query1Week.defineQuery(dataStream));

        Query2 query2Month = new Query2(WindowSize.MONTHLY);
        sinkableStream.add(query2Month.defineQuery(dataStream));

        Query2 query2Week = new Query2(WindowSize.WEEKLY);
        sinkableStream.add(query2Week.defineQuery(dataStream));

        Query3 query3Hour = new Query3(WindowSize.ONE_HOUR);
        sinkableStream.add(query3Hour.defineQuery(dataStream));

        Query3 query3TwoHour = new Query3(WindowSize.TWO_HOUR);
        sinkableStream.add(query3TwoHour.defineQuery(dataStream));

        if(!Constants.MOCK){
            addSink(sinkableStream);
        }

        try {
            executionEnvironment.execute("Queries");
        } catch (Exception e) {
            System.out.println("An error occurred executing the Flink Job.");
            e.printStackTrace();
        }

    }

    private DataStream<NavalData> registerSource(){

        DataStream<String> dataStream = null;

        if(Constants.MOCK){

            dataStream = executionEnvironment.readTextFile("test_dataset/ordered_dataset_extended.csv");

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

            return new NavalData(formattedDate, values[1], values[2], Float.parseFloat(values[4]), Float.parseFloat(values[5]), values[10]);
        });



    }

    private DataStream<NavalData> preproccessing(DataStream<NavalData> dataStream){

        DataStream<Tuple2<NavalData, Long>> dataStreamTS = filterStream(dataStream);

        dataStreamTS = addCells(dataStreamTS);

        dataStreamTS = setSea(dataStreamTS);

        dataStreamTS = setTimeSlot(dataStreamTS);

        dataStream = dataStreamTS.map(new PreprocessingLatencyTracker());

        dataStream = assignTimestamp(dataStream);

        return dataStream;


    }

    private DataStream<Tuple2<NavalData, Long>> filterStream(DataStream<NavalData> dataStream){

        DataStream<Tuple2<NavalData, Long>> dataStreamTS = dataStream.map(new MapFunction<NavalData, Tuple2<NavalData, Long>>() {
            @Override
            public Tuple2<NavalData, Long> map(NavalData navalData) throws Exception {
                return new Tuple2<>(navalData, System.currentTimeMillis());
            }
        });

        dataStreamTS = dataStreamTS.filter((FilterFunction<Tuple2<NavalData, Long>>) nav ->
                    nav.f0.getLat() >= Constants.MIN_LAT &&
                    nav.f0.getLat() <= Constants.MAX_LAT &&
                    nav.f0.getLon() >= Constants.MIN_LON &&
                    nav.f0.getLon() <= Constants.MAX_LON);


        return dataStreamTS;
    }

    private DataStream<Tuple2<NavalData, Long>> addCells(DataStream<Tuple2<NavalData, Long>> dataStream){


         dataStream = dataStream.map(new MapFunction<Tuple2<NavalData, Long>, Tuple2<NavalData, Long>>() {
             @Override
             public Tuple2<NavalData, Long> map(Tuple2<NavalData, Long> navalData) throws Exception {

                 float lon = navalData.f0.getLon();
                 float lat = navalData.f0.getLat();
                 String lat_component = null;
                 String lon_component = null;

                 if (lat >= 32 && lat < 33.3) {
                     lat_component = "A";
                 } else if (lat >= 33.3 && lat < 34.6) {
                     lat_component = "B";
                 } else if (lat >= 34.6 && lat < 35.9) {
                     lat_component = "C";
                 } else if (lat >= 35.9 && lat < 37.2) {
                     lat_component = "D";
                 } else if (lat >= 37.2 && lat < 38.5) {
                     lat_component = "E";
                 } else if (lat >= 38.5 && lat < 39.8) {
                     lat_component = "F";
                 } else if (lat >= 39.8 && lat < 41.1) {
                     lat_component = "G";
                 } else if (lat >= 41.1 && lat < 42.4) {
                     lat_component = "H";
                 } else if (lat >= 42.4 && lat < 43.7) {
                     lat_component = "I";
                 } else if (lat >= 43.7 && lat < 45) {
                     lat_component = "J";
                 }

                 float lon_start = Constants.MIN_LON;
                 float lon_end = lon_start + 1.075f;

                 for (Integer i = 1; i < 41; i++) {
                     if (lon >= lon_start && lon < lon_end) {
                         lon_component = i.toString();
                         break;
                     }

                     lon_start = lon_end;
                     lon_end = lon_end + 1.075f;
                 }


                 if (lat_component == null)
                     throw new Exception("Error in latitude values inserted: " + navalData.f0.getLat());

                 if (lon_component == null)
                     throw new Exception("Error in longitude values inserted: " + navalData.f0.getLon());

                 String cell = lat_component + lon_component;

                 navalData.f0.setCell(cell);

                 return navalData;
             }
         });

        return dataStream;
    }


    private DataStream<Tuple2<NavalData, Long>> setSea(DataStream<Tuple2<NavalData, Long>> dataStream){

        dataStream = dataStream.map(new MapFunction<Tuple2<NavalData, Long>, Tuple2<NavalData, Long>>() {
            @Override
            public Tuple2<NavalData, Long> map(Tuple2<NavalData, Long> navalData) throws Exception {

                float lon = navalData.f0.getLon();

                Seas sea;
                if (lon <= Constants.WEST_SEA_END) sea = Seas.WESTERN_MEDITERANEAN_SEA;
                else sea = Seas.EASTERN_MEDITERANEAN_SEA;

                navalData.f0.setSea(sea);

                return navalData;
            }
        });

        return dataStream;
    }

    private DataStream<Tuple2<NavalData, Long>> setTimeSlot(DataStream<Tuple2<NavalData, Long>> dataStream){

        dataStream = dataStream.map(new MapFunction<Tuple2<NavalData, Long>, Tuple2<NavalData, Long>>() {
            @Override
            public Tuple2<NavalData, Long> map(Tuple2<NavalData, Long> navalData) throws Exception {

                SimpleDateFormat hourFormat = new SimpleDateFormat("HH:mm");

                String date = hourFormat.format(navalData.f0.getTs());

                TimeSlot slot;

                if (date.compareTo(Constants.TIME_SLOT_END) < 0) slot = TimeSlot.BEFORE_NOON;
                else slot = TimeSlot.AFTER_NOON;

                navalData.f0.setTimeSlot(slot);

                return navalData;
            }
        });

        return dataStream;
    }

    private DataStream<NavalData> assignTimestamp(DataStream<NavalData> dataStream){

        WatermarkStrategy<NavalData> watermarkStrategy = WatermarkStrategy
                .<NavalData>forBoundedOutOfOrderness(Constants.ALLOWED_LATENESS);

        if(Constants.MOCK)
                watermarkStrategy = watermarkStrategy.withTimestampAssigner((event, timestamp) -> event.getTs().getTime());

        dataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);

        return dataStream;
    }

    private void addSink(List<SingleOutputStreamOperator<String>> datastreams){

        for(SingleOutputStreamOperator<String> stream : datastreams) {

            FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(Constants.OUTPUT_TOPIC_NAME, new SimpleStringSchema(), properties);

            stream.addSink(kafkaSink);
        }

    }


    //Function used to Test Windowing (not Called or Referenced)
    private void testStream(DataStream<NavalData> dataStream){

        SingleOutputStreamOperator<String> outputStreamOperator =
                dataStream
                        .map((MapFunction<NavalData, String>) NavalData::getFormattedTs)
                        .windowAll(SlidingEventTimeWindows.of(Time.days(7L), Time.days(7L), Time.seconds(Constants.TEST_WINDOW_OFFSET)))
                        .reduce((ReduceFunction<String>) (s, t1) -> s + "######" + t1);

        outputStreamOperator.print();
    }





}

