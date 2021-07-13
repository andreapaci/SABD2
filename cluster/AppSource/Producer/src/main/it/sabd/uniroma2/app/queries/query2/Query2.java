package it.sabd.uniroma2.app.queries.query2;

import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.enums.WindowSize;
import it.sabd.uniroma2.app.queries.QueryAbstract;
import it.sabd.uniroma2.app.util.Constants;
import it.sabd.uniroma2.app.util.QueryLatencyTracker;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.Iterator;

public class Query2 extends QueryAbstract {


    public Query2(WindowSize windowSize) {
        super(windowSize);
        this.tag = Constants.QUERY2_OUTPUT_TAG + this.tag;
    }

    @Override
    public SingleOutputStreamOperator<String> defineQuery(DataStream<NavalData> dataStream) {

        SingleOutputStreamOperator<String> output =
                dataStream
                .keyBy(NavalData::getStringSea)
                .window(SlidingEventTimeWindows.of(this.windowSizeTime, this.slidingFactor, this.offset))
                .aggregate(new Query2Aggregator(),
                        new ProcessWindowFunction<Query2Result, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Query2Result> iterable, Collector<String> collector) throws Exception {

                                SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");

                                String output = format.format(context.window().getStart()) + "," + s;

                                Iterator<Query2Result> iterator = iterable.iterator();
                                Query2Result tuple = iterator.next();

                                output += tuple.returnLists();

                                collector.collect(output);
                            }
                        })
                .map(new QueryLatencyTracker("Query2" + window.toString()));

        output = appendTag(output);

        if(Constants.PRINT_FLINK_OUTPUT) output.print();

        return output;
    }


}
