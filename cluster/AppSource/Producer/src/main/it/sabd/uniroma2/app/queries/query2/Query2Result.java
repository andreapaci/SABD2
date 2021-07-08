package it.sabd.uniroma2.app.queries.query2;

import it.sabd.uniroma2.app.enums.TimeSlot;
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
        output += TimeSlot.BEFORE_NOON + "," + listCellsBeforeNoon.toString();
        output += TimeSlot.AFTER_NOON + "," + listCellsAfterNoon.toString();

        return output;
    }
}
