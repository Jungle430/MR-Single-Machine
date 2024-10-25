package com.Jungle.person.models;


import com.Jungle.person.util.GsonUtil;

public record KeyValue(String key, String value) implements Comparable<KeyValue> {
    public String toJson() {
        return GsonUtil.beanToJson(this);
    }

    public String toJsonLine() {
        return GsonUtil.beanToJson(this) + '\n';
    }

    @Override
    public int compareTo(KeyValue o) {
        return key.compareTo(o.key);
    }
}
