package it.sabd.uniroma2.app.queries.query3;

import it.sabd.uniroma2.app.util.Utils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

public class Query3ProcessWindowFunction extends ProcessWindowFunction<Float, NavigationScore, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Float> iterable, Collector<NavigationScore> collector) throws Exception {

        NavigationScore res = new NavigationScore();
        float navigationScore = iterable.iterator().next();
        res.setTs(Utils.formatDateToStringMinutes(new Date(context.window().getStart())));
        res.setTripId(s);
        res.setNavigaionScore(String.valueOf(navigationScore));
        collector.collect(res);

    }
}
