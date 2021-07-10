package it.sabd.uniroma2.app.queries.query3;

import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.util.Constants;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Query3Aggregator implements AggregateFunction<NavalData, Tuple4<String,Long,Float,Float>, Float> {

    private HashMap<String, Tuple3<Long, Float, Float>> map;

    public Query3Aggregator(HashMap<String, Tuple3<Long, Float, Float>> globalMap){
        this.map = globalMap;
    }

    @Override
    public Tuple4<String,Long,Float,Float> createAccumulator()  {
        Tuple4<String,Long,Float,Float> accumulator = new Tuple4<>();
        accumulator.f0 = null;
        accumulator.f1 = 0L;
        accumulator.f2 = 0f;
        accumulator.f3 = 0f;
        return accumulator;
    }

    @Override
    public Tuple4<String,Long,Float,Float> add(NavalData navalData, Tuple4<String,Long,Float,Float> accumulator) {

        String tripId = navalData.getTripId();
        if(this.map.get(tripId)==null){
            map.put(tripId,new Tuple3<>(navalData.getTs().getTime(), navalData.getLon(),navalData.getLat()));
        }else if(this.map.get(tripId).f0 > navalData.getTs().getTime()){
            map.put(tripId,new Tuple3<>(navalData.getTs().getTime(), navalData.getLon(),navalData.getLat()));
        }

        long navalDate = navalData.getTs().getTime();
        if(navalDate >= accumulator.f1){
            accumulator.f0 = tripId;
            accumulator.f1 = navalDate;
            accumulator.f2 = navalData.getLon();
            accumulator.f3 = navalData.getLat();
        }
        return accumulator;
    }

    @Override
    public Float getResult(Tuple4<String,Long,Float,Float> accumulator) {

        Tuple3<Long, Float, Float> lonLatStart = map.get(accumulator.f0);

        if(Constants.PRINT_FLINK_OUTPUT) {

            System.out.println("Printing starting LON, LAT: " + lonLatStart.f1 + ", " + lonLatStart.f2);

            System.out.println("------------------------ CROSS-WINDOW STATE ON --------------------------------");
            for (String s : map.keySet().toArray(new String[0])) {
                System.out.println(s + "[" + map.get(s).f0 + "; " + map.get(s).f1 + "; " + map.get(s).f2 + "]");
            }
            System.out.println("------------------------ CROSS-WINDOW STATE OFF --------------------------------");

        }

        return (float) Math.sqrt((lonLatStart.f1 - accumulator.f2) * (lonLatStart.f1 - accumulator.f2) +
                                                    (lonLatStart.f2 - accumulator.f3) * (lonLatStart.f2 - accumulator.f3));


    }

    @Override
    public Tuple4<String, Long, Float, Float> merge(Tuple4<String, Long, Float, Float> stringLongFloatFloatTuple4, Tuple4<String, Long, Float, Float> acc1) {
        return null;
    }


}
