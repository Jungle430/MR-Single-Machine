package com.Jungle.person.tools.impl;

import com.Jungle.person.tools.ReduceFunction;

import java.util.List;

/**
 * @author Jungle
 */
public class WcReduce implements ReduceFunction {
    @Override
    public String ReduceF(String key_, List<String> value) {
        return Integer.toString(value.size());
    }
}
