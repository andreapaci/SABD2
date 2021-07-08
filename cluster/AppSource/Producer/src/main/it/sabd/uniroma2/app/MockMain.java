package it.sabd.uniroma2.app;

import it.sabd.uniroma2.app.flink.FlinkTopology;
import it.sabd.uniroma2.app.util.Constants;

public class MockMain {

    public static void main(String[] args){

        System.out.println("Mocking system... Only flink running");

        Constants.MOCK = true;

        FlinkTopology flinkTopology = new FlinkTopology();

        flinkTopology.defineTopology();

    }
}
