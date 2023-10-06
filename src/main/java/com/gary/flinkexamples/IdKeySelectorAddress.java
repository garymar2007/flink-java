package com.gary.flinkexamples;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class IdKeySelectorAddress implements KeySelector<Tuple3<Integer, String, String>, Integer>  {
    @Override
    public Integer getKey(Tuple3<Integer, String, String> integerStringStringTuple3) throws Exception {
        return integerStringStringTuple3.f0;
    }
}
