package it.sabd.uniroma2.app.queries.query2;

import it.sabd.uniroma2.app.entity.NavalData;
import org.apache.flink.api.common.functions.AggregateFunction;

public class Query2Aggregator implements AggregateFunction<NavalData, Query2Accumulator, Query2Result> {


    @Override
    public Query2Accumulator createAccumulator() {
        return new Query2Accumulator();
    }

    @Override
    public Query2Accumulator add(NavalData navalData, Query2Accumulator query2Accumulator) {
        query2Accumulator.addNavalData(navalData);
        return query2Accumulator;
    }

    @Override
    public Query2Result getResult(Query2Accumulator query2Accumulator) {

        Query2Result result = new Query2Result();
        result.populateBeforeNoon(query2Accumulator);
        result.populateAfterNoon(query2Accumulator);

        return result;
    }

    @Override
    public Query2Accumulator merge(Query2Accumulator query2Accumulator, Query2Accumulator acc1) {
        return null;
    }
}
