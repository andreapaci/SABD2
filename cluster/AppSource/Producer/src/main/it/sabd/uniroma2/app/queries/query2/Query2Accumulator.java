package it.sabd.uniroma2.app.queries.query2;

import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.enums.TimeSlot;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class Query2Accumulator {

    private HashMap<String, ArrayList<String>> beforeNoonCells;
    private HashMap<String, ArrayList<String>> afterNoonCells;

    private SimpleDateFormat format;

    public Query2Accumulator(){
        beforeNoonCells = new HashMap<>();
        afterNoonCells = new HashMap<>();

        format = new SimpleDateFormat("yyyy/MM/dd");
    }


    public void addNavalData(NavalData navalData){
        if(navalData.getTimeSlot() == TimeSlot.BEFORE_NOON) appendToList(navalData, beforeNoonCells);
        else appendToList(navalData, afterNoonCells);

    }

    private void appendToList(NavalData navalData, HashMap<String, ArrayList<String>> cells){
        String cell = navalData.getCell();
        String date = format.format(navalData.getTs());
        String id = navalData.getId();
        ArrayList<String> list = cells.get(cell);
        String data = id + date;

        //TODO: usare hashmap invece che arraylist (key = nave giorno, value = nan)
        if (list == null) {
            list = new ArrayList<>();
            list.add(data);
            cells.put(cell, list);
        } else {
            if (!list.contains(data)) list.add(data);
        }

    }

    public HashMap<String, ArrayList<String>> getBeforeNoonCells() {
        return beforeNoonCells;
    }

    public void setBeforeNoonCells(HashMap<String, ArrayList<String>> beforeNoonCells) {
        this.beforeNoonCells = beforeNoonCells;
    }

    public HashMap<String, ArrayList<String>> getAfterNoonCells() {
        return afterNoonCells;
    }

    public void setAfterNoonCells(HashMap<String, ArrayList<String>> afterNoonCells) {
        this.afterNoonCells = afterNoonCells;
    }
}
