package it.sabd.uniroma2.app.queries.query3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class AccumulatorTop5 {

    private String ts;
    private ArrayList<Float> scores;
    private ArrayList<String> tripIds;

    public AccumulatorTop5(){
        this.scores = new ArrayList<>();
        this.tripIds = new ArrayList<>();
    }

    public void setMin(String ts, String tripId, float score){

        this.ts = ts;
        if(this.tripIds.size() < 5){
            this.scores.add(score);
            this.tripIds.add(tripId);
        }else{
            int minIndex = this.scores.indexOf(Collections.min(this.scores));
            if(this.scores.get(minIndex) < score){
                this.scores.set(minIndex,score);
                this.tripIds.set(minIndex,tripId);
            }
        }
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public ArrayList<Float> getScores() {
        return scores;
    }

    public void setScores(ArrayList<Float> scores) {
        this.scores = scores;
    }

    public ArrayList<String> getTripIds() {
        return tripIds;
    }

    public void setTripIds(ArrayList<String> tripIds) {
        this.tripIds = tripIds;
    }
}
