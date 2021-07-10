package it.sabd.uniroma2.app.queries.query1;

import it.sabd.uniroma2.app.util.Constants;
import it.sabd.uniroma2.app.entity.NavalData;
import it.sabd.uniroma2.app.enums.Seas;
import it.sabd.uniroma2.app.enums.WindowSize;
import it.sabd.uniroma2.app.queries.QueryAbstract;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class Query1 extends QueryAbstract {

    public Query1(WindowSize windowSize) {
        super(windowSize);
        this.tag = Constants.QUERY1_OUTPUT_TAG + this.tag;
    }


    @Override
    public SingleOutputStreamOperator<String> defineQuery(DataStream<NavalData> navalData) {


        navalData = navalData.filter((FilterFunction<NavalData>) navalData1 -> navalData1.getSea() == Seas.WESTERN_MEDITERANEAN_SEA);

        SingleOutputStreamOperator<String> output = navalData
                .keyBy(NavalData::getCell)
                .window(SlidingEventTimeWindows.of(this.windowSizeTime, this.slidingFactor, this.offset))
                .aggregate(new Query1Aggregator(), new Query1ProcessWindowFunction())
                .map(new Query1MapFunction(window));

        output = appendTag(output);

        if(Constants.PRINT_FLINK_OUTPUT) output.print();

        return output;
    }
}
