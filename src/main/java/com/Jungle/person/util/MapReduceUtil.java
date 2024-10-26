package com.Jungle.person.util;

import com.Jungle.person.config.MapReduceConfig;
import com.Jungle.person.tools.MapFunction;
import com.Jungle.person.tools.ReduceFunction;
import com.Jungle.person.tools.Worker;
import com.Jungle.person.tools.impl.MapWorker;
import com.Jungle.person.tools.impl.ReduceWorker;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author Jungle
 */
@Slf4j
public final class MapReduceUtil {
    private MapReduceUtil() {
    }

    public static void MapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f) {
        mapReduce(inputFileNames, map_f, reduce_f, null, null);
    }

    public static void MapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f, String intermediateFilePrefix) {
        mapReduce(inputFileNames, map_f, reduce_f, intermediateFilePrefix, null);
    }

    public static void MapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f, String intermediateFilePrefix, String outputFilePreFix) {
        mapReduce(inputFileNames, map_f, reduce_f, intermediateFilePrefix, outputFilePreFix);
    }

    private static void mapReduce(List<String> inputFileNames, MapFunction map_f, ReduceFunction reduce_f, String intermediateFilePrefix, String outputFilePreFix) {
        Objects.requireNonNull(inputFileNames);
        Objects.requireNonNull(map_f);
        Objects.requireNonNull(reduce_f);

        // Map
        List<Worker> mapWorkers = new ArrayList<>(inputFileNames.size());
        for (int i = 0; i < inputFileNames.size(); i++) {
            mapWorkers.add(new MapWorker(i, map_f, MapReduceConfig.N_REDUCE, inputFileNames.get(i), intermediateFilePrefix));
        }

        log.info("Map task started");
        List<CompletableFuture<Boolean>> mapTasks = new ArrayList<>(inputFileNames.size());
        for (Worker w : mapWorkers) {
            mapTasks.add(w.work());
        }
        for (int i = 0; i < mapTasks.size(); i++) {
            int store_i = i;
            mapTasks.get(i)
                    .thenAccept(x -> log.info("Map task:{}, result:{}", mapWorkers.get(store_i), x))
                    .exceptionally(e -> {
                        log.info("Map task:{}, exception:{}", mapWorkers.get(store_i), e.getMessage());
                        return null;
                    });
        }
        CompletableFuture<Void> allMapTasks = CompletableFuture.allOf(mapTasks.toArray(new CompletableFuture[0]));
        allMapTasks.join();
        log.info("Map task completed");

        // Reduce
        List<Worker> reduceWorkers = new ArrayList<>(MapReduceConfig.N_REDUCE);
        for (int i = 0; i < MapReduceConfig.N_REDUCE; i++) {
            reduceWorkers.add(new ReduceWorker(i, reduce_f, MapReduceConfig.N_MAP, intermediateFilePrefix, outputFilePreFix));
        }

        log.info("Reduce task started");
        List<CompletableFuture<Boolean>> reduceTasks = new ArrayList<>(MapReduceConfig.N_REDUCE);
        for (Worker w : reduceWorkers) {
            reduceTasks.add(w.work());
        }
        for (int i = 0; i < reduceTasks.size(); i++) {
            int store_i = i;
            reduceTasks.get(i)
                    .thenAccept(x -> log.info("Reduce task:{}, result:{}", reduceWorkers.get(store_i), x))
                    .exceptionally(e -> {
                        log.info("Reduce task:{}, exception:{}", reduceWorkers.get(store_i), e.getMessage());
                        return null;
                    });
        }

        CompletableFuture<Void> allReduceTasks = CompletableFuture.allOf(reduceTasks.toArray(new CompletableFuture[0]));
        allReduceTasks.join();
        log.info("All reduce tasks completed");
    }
}
