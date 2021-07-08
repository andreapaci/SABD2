package it.sabd.uniroma2.app.queries;

import it.sabd.uniroma2.app.util.Constants;
import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.enums.WindowSize;
import it.sabd.uniroma2.app.util.MapFunctionAppendTag;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class QueryAbstract {

    protected WindowSize window;
    protected Time windowSizeTime;
    protected Time slidingFactor;
    protected Time offset;
    protected String tag;


    public QueryAbstract(WindowSize windowSize) {

        this.window = windowSize;
        Tuple3<Time, Time, Time> windowProp = Constants.WINDOW_MAP.get(windowSize);
        this.windowSizeTime = windowProp.f0;
        this.slidingFactor = windowProp.f1;
        this.offset = windowProp.f2;

        tag = windowSize.toString() + Constants.OUTPUT_DIVIDER;

    }

    public abstract SingleOutputStreamOperator<String> defineQuery(DataStream<NavalData> navalData);

    protected SingleOutputStreamOperator<String> appendTag(SingleOutputStreamOperator<String> stream){
        return stream.map(new MapFunctionAppendTag(this.tag));
    }
}
