package com.Jungle.person.config;

import com.Jungle.person.tools.impl.MapWorker;
import com.Jungle.person.util.PropertiesUtil;

import java.util.Arrays;
import java.util.List;

/**
 * @author Jungle
 */
public final class MapReduceConfig {
    public static final List<String> INPUT_FILES;

    public static final int N_MAP;

    public static final int N_REDUCE;

    static {
        INPUT_FILES = Arrays.stream(PropertiesUtil.getProperty("mapreduce.files.input").split(",")).toList();
        N_MAP = INPUT_FILES.size();
        N_REDUCE = Integer.parseInt(PropertiesUtil.getProperty("mapreduce.reduce.num"));
    }

    public static final String INPUT_FILE_DEFAULT_NAME_PREFIX = "inputFile-";

    public static final String INTERMEDIATE_FILE_DEFAULT_NAME_PREFIX = "intermediateFile-";

    public static final String OUTPUT_FILE_DEFAULT_NAME_PREFIX = "mr-out-";

    public static String withMapWorkerTargetFolderPrefix(String fileName) {
        return String.format("%s-%s", MapWorker.class.getProtectionDomain().getCodeSource().getLocation().getPath(), fileName);
    }
}
