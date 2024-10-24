package com.Jungle.person.tools.impl;


import com.Jungle.person.models.KeyValue;
import com.Jungle.person.tools.MapFunction;
import com.Jungle.person.tools.Worker;
import lombok.extern.slf4j.Slf4j;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Slf4j
public class MapWorker implements Worker {
    private static final String INPUT_FILE_DEFAULT_NAME_PREFIX = "inputFile-";

    private static final String INTERMEDIATE_FILE_DEFAULT_NAME_PREFIX = "intermediateFile-";

    private final int id;

    private final MapFunction mapFunction;

    private final int reduceWorkerCount;

    private final String inputFileName;

    private final String intermediateFileNamePrefix;

    private static boolean resourceFileNotExists(String fileName) {
        return MapWorker.class.getClassLoader().getResource(fileName) == null;
    }

    private static void intermediateFileAbort(List<String> createdFiles) {
        createdFiles.stream()
                .map(File::new)
                .filter(File::exists)
                .forEach(file -> {
                    boolean ignore = file.delete();
                });
    }

    private List<List<KeyValue>> distributeKVa(List<KeyValue> kva) {
        List<List<KeyValue>> distributedKva = new ArrayList<>(reduceWorkerCount);
        for (int i = 0; i < reduceWorkerCount; i++) {
            distributedKva.add(new ArrayList<>(kva.size() / reduceWorkerCount));
        }
        for (KeyValue kv : kva) {
            distributedKva.get((kv.key().hashCode() & Integer.MAX_VALUE) % reduceWorkerCount).add(kv);
        }
        return distributedKva;
    }

    public MapWorker(int id, MapFunction mapFunction, int reduceWorkerCount) {
        this.id = id;
        this.mapFunction = mapFunction;
        this.reduceWorkerCount = reduceWorkerCount;
        this.inputFileName = String.format("%s%d", INPUT_FILE_DEFAULT_NAME_PREFIX, id);
        if (resourceFileNotExists(inputFileName)) {
            throw new IllegalArgumentException("Resource file " + inputFileName + " does not exist");
        }
        this.intermediateFileNamePrefix = String.format("%s%d-", INTERMEDIATE_FILE_DEFAULT_NAME_PREFIX, id);
    }

    public MapWorker(int id, MapFunction mapFunction, int reduceWorkerCount, String inputFileName) {
        this.id = id;
        this.mapFunction = mapFunction;
        this.reduceWorkerCount = reduceWorkerCount;
        this.inputFileName = inputFileName;
        if (resourceFileNotExists(inputFileName)) {
            throw new IllegalArgumentException("Resource file " + inputFileName + " does not exist");
        }
        this.intermediateFileNamePrefix = String.format("%s%d-", INTERMEDIATE_FILE_DEFAULT_NAME_PREFIX, id);
    }

    public MapWorker(int id, MapFunction mapFunction, int reduceWorkerCount, String inputFileName, String intermediateFileNamePrefix) {
        this.id = id;
        this.mapFunction = mapFunction;
        this.reduceWorkerCount = reduceWorkerCount;
        this.inputFileName = inputFileName;
        if (resourceFileNotExists(inputFileName)) {
            throw new IllegalArgumentException("Resource file " + inputFileName + " does not exist");
        }
        this.intermediateFileNamePrefix = intermediateFileNamePrefix;
    }

    @Override
    public String toString() {
        return "MapWorker{" +
                "id=" + id +
                ", mapFunction=" + mapFunction.getName() +
                ", reduceWorkerCount=" + reduceWorkerCount +
                ", inputFileName='" + inputFileName + '\'' +
                ", intermediateFileNamePrefix='" + intermediateFileNamePrefix + '\'' +
                '}';
    }

    @Override
    public Future<Boolean> work() {
        return CompletableFuture.supplyAsync(() -> {
            log.info("map task:{}", this);
            List<KeyValue> kva;
            try (InputStream inputStream = Optional
                    .ofNullable(
                            MapWorker.class.getClassLoader().getResourceAsStream(inputFileName)
                    )
                    .orElseThrow(
                            () -> new IllegalArgumentException("Input file " + inputFileName + " does not exist")
                    )
            ) {
                kva = mapFunction.MapF(inputFileName, new String(inputStream.readAllBytes()));
            } catch (SecurityException | IOException | IllegalArgumentException e) {
                log.error("process input file {} failed {}", inputFileName, e.getMessage());
                throw new RuntimeException(e);
            }

            List<List<KeyValue>> distributedKva = distributeKVa(kva);
            List<String> createdFiles = new ArrayList<>();
            boolean success = true;
            try {
                for (int i = 0; i < reduceWorkerCount; i++) {
                    try (OutputStream outputStream = new FileOutputStream(intermediateFileNamePrefix + i)) {
                        createdFiles.add(intermediateFileNamePrefix + i);
                        for (KeyValue kv : distributedKva.get(i)) {
                            outputStream.write(kv.toJson().getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }
            } catch (IOException e) {
                log.error("process intermediate file {} failed {}", inputFileName, e.getMessage());
                success = false;
            }

            if (!success) {
                intermediateFileAbort(createdFiles);
                return false;
            }

            return true;
        });
    }
}
