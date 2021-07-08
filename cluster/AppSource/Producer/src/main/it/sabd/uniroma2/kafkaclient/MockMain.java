package it.sabd.uniroma2.kafkaclient;

public class MockMain {

    public static void main(String[] args){

        System.out.println("Mocking system... Only flink running");

        Constants.MOCK = true;

        FlinkTopology flinkTopology = new FlinkTopology();

        flinkTopology.defineTopology();

    }
}
