package com.Jungle.person.util;

import com.Jungle.person.config.MapReduceConfig;
import com.Jungle.person.tools.MapFunction;
import com.Jungle.person.tools.ReduceFunction;
import com.Jungle.person.tools.Worker;
import com.Jungle.person.tools.impl.MapWorker;
import com.Jungle.person.tools.impl.ReduceWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Jungle
 */
public final class MapReduceUtil {
    private MapReduceUtil() {
    }

    public static void MapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f) throws ExecutionException, InterruptedException {
        mapReduce(inputFileNames, map_f, reduce_f, null, null);
    }

    public static void MapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f, String intermediateFilePrefix) throws ExecutionException, InterruptedException {
        mapReduce(inputFileNames, map_f, reduce_f, intermediateFilePrefix, null);
    }

    public static void MapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f, String intermediateFilePrefix, String outputFilePreFix) throws ExecutionException, InterruptedException {
        mapReduce(inputFileNames, map_f, reduce_f, intermediateFilePrefix, outputFilePreFix);
    }

    private static void mapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f, String intermediateFilePrefix, String outputFilePreFix) throws ExecutionException, InterruptedException {
        Objects.requireNonNull(inputFileNames);
        Objects.requireNonNull(map_f);
        Objects.requireNonNull(reduce_f);

        // Map
        List<Worker> mapWorkers = new ArrayList<>(inputFileNames.size());
        for (int i = 0; i < inputFileNames.size(); i++) {
            mapWorkers.add(new MapWorker(i, map_f, MapReduceConfig.N_REDUCE, inputFileNames.get(i), intermediateFilePrefix));
        }

        List<CompletableFuture<Boolean>> mapTasks = new ArrayList<>(inputFileNames.size());
        for (Worker w : mapWorkers) {
            mapTasks.add(w.work());
        }
        for (CompletableFuture<Boolean> f : mapTasks) {
            boolean ignore = f.get();
        }

        // Reduce
        List<Worker> reduceWorkers = new ArrayList<>(MapReduceConfig.N_REDUCE);
        for (int i = 0; i < MapReduceConfig.N_REDUCE; i++) {
            reduceWorkers.add(new ReduceWorker(i, reduce_f, MapReduceConfig.N_MAP, intermediateFilePrefix, outputFilePreFix));
        }

        List<CompletableFuture<Boolean>> reduceTasks = new ArrayList<>(MapReduceConfig.N_REDUCE);
        for (Worker w : reduceWorkers) {
            reduceTasks.add(w.work());
        }
        for (CompletableFuture<Boolean> f : reduceTasks) {
            boolean ignore = f.get();
        }
    }
}
