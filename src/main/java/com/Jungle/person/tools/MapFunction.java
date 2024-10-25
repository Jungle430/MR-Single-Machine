package com.Jungle.person.tools;

import com.Jungle.person.models.KeyValue;

import java.util.List;

/**
 * @author Jungle
 */
@FunctionalInterface
public interface MapFunction {
    List<KeyValue> MapF(String key, String value);

    default String getName() {
        return this.getClass().getName();
    }
}
