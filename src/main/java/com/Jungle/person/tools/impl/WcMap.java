package com.Jungle.person.tools.impl;

import com.Jungle.person.models.KeyValue;
import com.Jungle.person.tools.MapFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author Jungle
 */
public class WcMap implements MapFunction {
    @Override
    public List<KeyValue> MapF(String key_, String value) {
        return Arrays.stream(value.split("[^a-zA-Z]+"))
                .filter(s -> !s.isEmpty())
                .map(word -> new KeyValue(word, "1"))
                .toList();
    }
}
