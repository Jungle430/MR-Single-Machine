package com.Jungle.person.tools.impl;

import com.Jungle.person.tools.ReduceFunction;

import java.util.Collections;
import java.util.List;

/**
 * @author Jungle
 */
public class IndexerReduce implements ReduceFunction {
    @Override
    public String ReduceF(String key_, List<String> value) {
        Collections.sort(value);
        return String.format("%d %s", value.size(), String.join(",", value));
    }
}
