package com.Jungle.person;

import com.Jungle.person.config.MapReduceConfig;
import com.Jungle.person.tools.MapFunction;
import com.Jungle.person.tools.ReduceFunction;
import com.Jungle.person.tools.Worker;
import com.Jungle.person.tools.impl.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class Main {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        log.info("Start Wc");
        MapReduce(new WcMap(), new WcReduce(), "mr-out-0");
        log.info("End Wc");
    }

    private static void MapReduce(MapFunction map_f, ReduceFunction reduce_f, String file_name) throws ExecutionException, InterruptedException {
        List<Worker> mapWorkers = new ArrayList<>(MapReduceConfig.INPUT_FILES.size());
        for (int i = 0; i < MapReduceConfig.INPUT_FILES.size(); i++) {
            mapWorkers.add(new MapWorker(i, map_f, MapReduceConfig.N_REDUCE, MapReduceConfig.INPUT_FILES.get(i)));
        }
        List<Future<Boolean>> mapTasks = new ArrayList<>(MapReduceConfig.INPUT_FILES.size());
        for (Worker w : mapWorkers) {
            mapTasks.add(w.work());
        }
        for (Future<Boolean> f : mapTasks) {
            boolean ignore = f.get();
        }
    }
}