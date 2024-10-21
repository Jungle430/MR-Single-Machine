package com.Jungle.person.tools.impl;

import com.Jungle.person.models.KeyValue;
import com.Jungle.person.tools.MapFunction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IndexerMap implements MapFunction {
    @Override
    public List<KeyValue> MapF(String key, String value) {
        return Arrays.stream(value.split("[^a-zA-Z]+"))
                .filter(x -> !x.isEmpty())
                .distinct()
                .map(word -> new KeyValue(word, key))
                .collect(Collectors.toList());
    }
}
