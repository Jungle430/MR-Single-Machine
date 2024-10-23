package com.Jungle.person.config;

import com.Jungle.person.util.PropertiesUtil;

import java.util.Arrays;
import java.util.List;

public class MapReduceConfig {
    public static final List<String> INPUT_FILES;

    public static final int N_MAP;

    public static final int N_REDUCE;

    static {
        INPUT_FILES = Arrays.stream(PropertiesUtil.getProperty("mapreduce.files.input").split(",")).toList();
        N_MAP = Integer.parseInt(PropertiesUtil.getProperty("mapreduce.map.num"));
        N_REDUCE = Integer.parseInt(PropertiesUtil.getProperty("mapreduce.reduce.num"));
    }
}
