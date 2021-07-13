package it.sabd.uniroma2.app.util;

import it.sabd.uniroma2.app.entity.NavalData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

public class PreprocessingLatencyTracker extends RichMapFunction<Tuple2<NavalData,Long>, NavalData> {

    private transient long operatorLatency = 0;


    @Override
    public void open(Configuration config) {

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Latency_Preprocessing", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return operatorLatency;
                    }
                });


        
    }

    @Override
    public NavalData map(Tuple2<NavalData, Long> value) throws Exception {

        operatorLatency = System.currentTimeMillis() - value.f1;

        //System.out.println("Latency Preprocssing: " + operatorLatency);

        return value.f0;
    }
}