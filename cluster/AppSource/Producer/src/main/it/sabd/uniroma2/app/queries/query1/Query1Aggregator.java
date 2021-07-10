package it.sabd.uniroma2.app.queries.query1;

import it.sabd.uniroma2.app.entity.NavalData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class Query1Aggregator implements AggregateFunction<NavalData, HashMap<String,Integer>, ArrayList<String>> {

    private SimpleDateFormat format;

    @Override
    public HashMap<String, Integer> createAccumulator()  {
        format = new SimpleDateFormat("yyyy/MM/dd");
        return new HashMap<>();
    }

    @Override
    public HashMap<String, Integer> add(NavalData navalData, HashMap<String, Integer> accumulator) {

        String date = format.format(navalData.getTs());
        String key = date + navalData.getId();
        accumulator.put(key, Integer.parseInt(navalData.getShipType()));

        return accumulator;
    }

    @Override
    public ArrayList<String> getResult(HashMap<String, Integer> accumulator) {

        ArrayList<String> result = new ArrayList<>();

        int count_type_35 = Collections.frequency(accumulator.values(), 35);

        int count_type_60_69 = 0;

        for (int i = 60; i < 70; i++) {
            int count = Collections.frequency(accumulator.values(), i);
            count_type_60_69 = count_type_60_69 + count;
        }

        int count_type_70_79 = 0;

        for (int i = 70; i < 80; i++) {
            int count = Collections.frequency(accumulator.values(), i);
            count_type_70_79 = count_type_70_79 + count;
        }

        int count_type_other = accumulator.size() - count_type_35 - count_type_60_69 - count_type_70_79;

        result.add(String.valueOf(count_type_35));
        result.add(String.valueOf(count_type_60_69));
        result.add(String.valueOf(count_type_70_79));
        result.add(String.valueOf(count_type_other));

        return result;
    }

    @Override
    public HashMap<String, Integer> merge(HashMap<String, Integer> stringIntegerHashMap, HashMap<String, Integer> acc1) {
        return null;
    }

}
