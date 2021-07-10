package it.sabd.uniroma2.app.queries.query2;

import it.sabd.uniroma2.app.enums.TimeSlot;
import it.sabd.uniroma2.app.util.Constants;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class Query2Result {

    private ArrayList<Tuple2<String, Integer>> listCellsBeforeNoon;
    private ArrayList<Tuple2<String, Integer>> listCellsAfterNoon;


    public Query2Result(){
        listCellsBeforeNoon = new ArrayList<>();
        listCellsAfterNoon = new ArrayList<>();
    }

    public void populateBeforeNoon(Query2Accumulator accumulator){

        populateList(listCellsBeforeNoon, accumulator.getBeforeNoonCells());
    }


    public void populateAfterNoon(Query2Accumulator accumulator){

        populateList(listCellsAfterNoon, accumulator.getAfterNoonCells());
    }

    private void populateList(ArrayList<Tuple2<String, Integer>> listCells,
                              HashMap<String, ArrayList<String>> hashMap){


        Iterator<String> it = hashMap.keySet().iterator();
        while (it.hasNext()) {
            String cell = it.next();
            listCells.add(new Tuple2<String, Integer>(cell, hashMap.get(cell).size()));

        }


    }


    public String returnLists(){

        String output = ",";

        output += TimeSlot.BEFORE_NOON + "," + computeLeaderBoard(listCellsBeforeNoon);
        output += TimeSlot.AFTER_NOON + "," + computeLeaderBoard(listCellsAfterNoon);

        return output;
    }


    private String computeLeaderBoard(ArrayList<Tuple2<String, Integer>> list){

        String leaderBoard = "";
        for(int i = 0; i < Constants.QUERY2_TOP; i++){
            leaderBoard += getAndRemoveMax(list);
        }

        return leaderBoard;

    }

    private String getAndRemoveMax(ArrayList<Tuple2<String, Integer>> list){

        if(list.isEmpty()) return "";


        Tuple2<String, Integer> max = list.get(0);


        for(Tuple2<String, Integer> e : list){
            if(max.f1 < e.f1) max = e;
        }

        String output = max.f0 + ";";

        list.remove(max);

        return output;
    }
}
