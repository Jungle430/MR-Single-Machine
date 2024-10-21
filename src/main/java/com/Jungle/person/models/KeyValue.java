package com.Jungle.person.models;


public record KeyValue(String key, String value) implements Comparable<KeyValue> {
    @Override
    public int compareTo(KeyValue o) {
        return 0;
    }
}
