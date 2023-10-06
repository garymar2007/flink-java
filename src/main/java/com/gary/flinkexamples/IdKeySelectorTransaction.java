package com.gary.flinkexamples;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class IdKeySelectorTransaction implements KeySelector<Tuple3<Integer, String, String>, Integer> {

    @Override
    public Integer getKey(Tuple3<Integer, String, String> value) throws Exception {
        return value.f0;
    }
}
