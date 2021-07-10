package it.sabd.uniroma2.app.queries.query3;

import it.sabd.uniroma2.app.entity.NavalData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.Comparator;

public class Query3AggregatorTop5 implements AggregateFunction<NavigationScore, AccumulatorTop5, String> {


    @Override
    public AccumulatorTop5 createAccumulator() {
        return new AccumulatorTop5();
    }

    @Override
    public AccumulatorTop5 add(NavigationScore navigationScore, AccumulatorTop5 accumulatorTop5) {

        accumulatorTop5.setMin(navigationScore.getTs(),navigationScore.getTripId(),Float.parseFloat(navigationScore.getNavigaionScore()));
        return accumulatorTop5;
    }

    @Override
    public String getResult(AccumulatorTop5 accumulatorTop5) {
        String result = accumulatorTop5.getTs() ;
        ArrayList<Float> scores = accumulatorTop5.getScores();
        ArrayList<String> tripIds = accumulatorTop5.getTripIds();
        ArrayList<Tuple2<String,Float>> results = new ArrayList<Tuple2<String, Float>>();
        if(tripIds.size()>0) {
            for(int i=0; i<scores.size();i++){
                results.add(new Tuple2<>(tripIds.get(i),scores.get(i)));
            }

            results.sort(Comparator.comparing(field -> field.f1,Comparator.reverseOrder()));

            for (int i = 0; i < results.size(); i++) {
                result = result + "," + results.get(i).f0 + "," + results.get(i).f1;
            }
        }
        return result;
    }

    @Override
    public AccumulatorTop5 merge(AccumulatorTop5 accumulatorTop5, AccumulatorTop5 acc1) {
        return null;
    }
}
