package com.Jungle.person.tools;

import java.util.List;

@FunctionalInterface
public interface ReduceFunction {
    String ReduceF(String key, List<String> value);
}