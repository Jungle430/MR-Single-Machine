package com.Jungle.person.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public final class GsonUtil {
    private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private GsonUtil() {
    }

    public static <T> String beanToJson(T bean) {
        return gson.toJson(bean);
    }

    public static <T> T jsonToBean(String jsonString, Class<T> cls) throws JsonSyntaxException {
        return gson.fromJson(jsonString, cls);
    }
}
