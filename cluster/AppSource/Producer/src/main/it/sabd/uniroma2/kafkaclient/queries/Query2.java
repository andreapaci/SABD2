package it.sabd.uniroma2.kafkaclient.queries;

import it.sabd.uniroma2.kafkaclient.Constants;
import it.sabd.uniroma2.kafkaclient.entity.NavalData;
import it.sabd.uniroma2.kafkaclient.enums.WindowSize;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;

public class Query2 extends QueryAbstract{


    public Query2(WindowSize windowSize) {
        super(windowSize);
        this.tag = "Query2" + this.tag;
    }

    @Override
    public SingleOutputStreamOperator<String> defineQuery(DataStream<NavalData> dataStream) {

        SingleOutputStreamOperator<String> output =
                dataStream
                .keyBy((KeySelector<NavalData, String>) navalData -> navalData.getSea().toString())
                .window(SlidingEventTimeWindows.of(Time.days(7L), Time.days(7L), Time.seconds(Constants.WINDOW_OFFSET)))
                .aggregate(new Query2Aggregator(),
                        new ProcessWindowFunction<Query2Result, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Query2Result> iterable, Collector<String> collector) throws Exception {

                                SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");

                                String output = format.format(context.window().getStart()) + "," + s;

                                Iterator<Query2Result> iterator = iterable.iterator();
                                Query2Result tuple = iterator.next();

                                output += tuple.returnLists();
                                output = tag + output;

                                collector.collect(output);
                            }
                        });
        return output;
    }


}
