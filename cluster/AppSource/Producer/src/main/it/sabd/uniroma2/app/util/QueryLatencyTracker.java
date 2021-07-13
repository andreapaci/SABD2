package it.sabd.uniroma2.app.util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

public class QueryLatencyTracker extends RichMapFunction<String, String> {

    private String query;
    private transient long operatorLatency = 0;
    private transient long counter;
    private long start;


    public QueryLatencyTracker(String query){
        this.query = query;


    }

    @Override
    public void open(Configuration config) {

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Latency_" + query, new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return operatorLatency;
                    }
                });
        start = System.currentTimeMillis();
        counter = 0;


    }

    @Override
    public String map(String s) throws Exception {

        counter++;

        operatorLatency = (System.currentTimeMillis() - start)/counter;

        //System.out.println("Latency " + query + ": " + Long.toString(operatorLatency));


        return s;
    }
}
