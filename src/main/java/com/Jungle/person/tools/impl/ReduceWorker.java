package com.Jungle.person.tools.impl;

import com.Jungle.person.tools.ReduceFunction;
import com.Jungle.person.tools.Worker;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;

@Slf4j
public class ReduceWorker implements Worker {
    private static final String OUTPUT_FILE_DEFAULT_NAME_PREFIX = "mr-out";

    private final int id;

    private final ReduceFunction reduceFunction;

    private final int mapWorkerCount;

    private final String outputFileName;

    public ReduceWorker(int id, ReduceFunction reduceFunction, int mapWorkerCount, String outputFileName) {
        this.reduceFunction = Objects.requireNonNull(reduceFunction);
        this.outputFileName = Optional.of(outputFileName).orElse(String.format("%s-%d", OUTPUT_FILE_DEFAULT_NAME_PREFIX, id));
        this.id = id;
        this.mapWorkerCount = mapWorkerCount;
        log.info("created ReduceWorker: {}", this);
    }

    @Override
    public String toString() {
        return "ReduceWorker {id=" + id + ", mapWorkerCount=" + mapWorkerCount + "}";
    }

    @Override
    public Future<Boolean> work() {
        return null;
    }
}
