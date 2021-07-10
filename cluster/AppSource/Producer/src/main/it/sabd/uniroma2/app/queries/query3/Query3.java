package it.sabd.uniroma2.app.queries.query3;

import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.enums.Seas;
import it.sabd.uniroma2.app.enums.WindowSize;
import it.sabd.uniroma2.app.queries.QueryAbstract;
import it.sabd.uniroma2.app.queries.query1.Query1Aggregator;
import it.sabd.uniroma2.app.queries.query1.Query1MapFunction;
import it.sabd.uniroma2.app.queries.query1.Query1ProcessWindowFunction;
import it.sabd.uniroma2.app.util.Constants;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Query3 extends QueryAbstract {

    private static HashMap<String, Tuple3<Long, Float, Float>> tripIdLongLatMap = new HashMap<>();

    public Query3(WindowSize windowSize) {
        super(windowSize);
        this.tag = Constants.QUERY3_OUTPUT_TAG + this.tag;
    }

    @Override
    public SingleOutputStreamOperator<String> defineQuery(DataStream<NavalData> navalData) {



        SingleOutputStreamOperator<NavigationScore> navigationScores = navalData
                .keyBy(NavalData::getTripId)
                .window(SlidingEventTimeWindows.of(this.windowSizeTime, this.slidingFactor, this.offset))
                .aggregate(new Query3Aggregator(tripIdLongLatMap), new Query3ProcessWindowFunction());


        if(Constants.PRINT_FLINK_OUTPUT) navigationScores.print();

        SingleOutputStreamOperator<String> output = navigationScores.keyBy(NavigationScore::getTs)
                .window(SlidingEventTimeWindows.of(this.windowSizeTime, this.slidingFactor, this.offset))
                .aggregate(new Query3AggregatorTop5()).setParallelism(1);


        output = appendTag(output);

        if(Constants.PRINT_FLINK_OUTPUT) output.print();

        return output;
    }
}
