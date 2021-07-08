package it.sabd.uniroma2.app.queries.query1;

import it.sabd.uniroma2.app.util.Utils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

public class Query1ProcessWindowFunction extends ProcessWindowFunction<ArrayList<String>, ArrayList<String>,
        String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<ArrayList<String>> iterable, Collector<ArrayList<String>> collector) throws Exception {

        ArrayList<String> res = iterable.iterator().next();
        res.add(s);
        res.add(Utils.formatDateToString(new Date(context.window().getStart())));
        collector.collect(res);

    }
}
