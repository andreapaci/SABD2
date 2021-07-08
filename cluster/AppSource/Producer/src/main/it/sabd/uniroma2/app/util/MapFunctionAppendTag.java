package it.sabd.uniroma2.app.util;

import org.apache.flink.api.common.functions.MapFunction;

public class MapFunctionAppendTag implements MapFunction<String, String> {

    private String tag;

    public MapFunctionAppendTag(String tag){ this.tag = tag; }

    @Override
    public String map(String s) throws Exception {
        return tag + s;
    }
}
