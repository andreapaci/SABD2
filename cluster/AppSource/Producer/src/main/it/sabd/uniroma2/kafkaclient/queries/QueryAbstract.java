package it.sabd.uniroma2.kafkaclient.queries;

import akka.stream.impl.Concat;
import it.sabd.uniroma2.kafkaclient.Constants;
import it.sabd.uniroma2.kafkaclient.entity.NavalData;
import it.sabd.uniroma2.kafkaclient.enums.WindowSize;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;

public abstract class QueryAbstract {

    protected Time windowSize;
    protected Time slidingFactor;
    protected Time offset;
    protected String tag;


    public QueryAbstract(WindowSize windowSize) {
        Tuple3<Time, Time, Time> windowProp = Constants.WINDOW_MAP.get(windowSize);
        this.windowSize = windowProp.f0;
        this.slidingFactor = windowProp.f1;
        this.offset = windowProp.f2;

        tag += windowSize.toString() + Constants.OUTPUT_DIVIDER;

    }

    public abstract SingleOutputStreamOperator<String> defineQuery(DataStream<NavalData> navalData);
}
