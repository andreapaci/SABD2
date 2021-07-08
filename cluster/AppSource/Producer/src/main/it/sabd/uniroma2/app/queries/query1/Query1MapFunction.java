package it.sabd.uniroma2.app.queries.query1;

import it.sabd.uniroma2.app.enums.WindowSize;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

public class Query1MapFunction implements MapFunction<ArrayList<String>, String> {

    private WindowSize windowSize;

    public Query1MapFunction(WindowSize windowSize) {
        this.windowSize = windowSize;

    }

    @Override
    public String map(ArrayList<String> strings) throws Exception {

        return mapOutputToString(strings);

    }

    public String mapOutputToString(ArrayList<String> strings){

        float divider;

        if(windowSize == WindowSize.WEEKLY){
            divider = 7;
        }else if( windowSize == WindowSize.MONTHLY){
            divider = 28;
        } else {
            System.out.println("Something wrong happened while transforming Query 1 output to string!");

            return null;
        }

        String count_35 = String.valueOf((float) (Integer.parseInt(strings.get(0))/divider));
        String count_60_69 = String.valueOf((float) (Integer.parseInt(strings.get(1))/divider));
        String count_70_79 = String.valueOf((float) (Integer.parseInt(strings.get(2))/divider));
        String count_others = String.valueOf((float) (Integer.parseInt(strings.get(3))/divider));

        String result = strings.get(5) + "," + strings.get(4) + "," + "SHIP_TYPE=35" + "," + count_35 + "," +
                "SHIP_TYPE=60-69" + "," + count_60_69 + "," + "SHIP_TYPE=70-79" + "," + count_70_79 + "," +
                "SHIP_TYPE=others" + "," + count_others;

        return result;
    }
}
