package com.Jungle.person.tools.impl;


import com.Jungle.person.models.KeyValue;
import com.Jungle.person.tools.MapFunction;
import com.Jungle.person.tools.Worker;
import lombok.extern.slf4j.Slf4j;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Slf4j
public class MapWorker implements Worker {
    private static final String INPUT_FILE_DEFAULT_NAME_PREFIX = "inputFile";

    private static final String INTERMEDIATE_FILE_DEFAULT_NAME_PREFIX = "intermediateFile-";

    private final int id;

    private final MapFunction mapFunction;

    private final int reduceWorkerCount;

    private final String inputFileName;

    private final String intermediateFileNamePrefix;

    public MapWorker(int id, MapFunction mapFunction, int reduceWorkerCount, String inputFileName, String intermediateFileNamePrefix) {
        this.mapFunction = Objects.requireNonNull(mapFunction);
        this.inputFileName = Optional.of(inputFileName).orElse(String.format("%s-%d", INPUT_FILE_DEFAULT_NAME_PREFIX, id));
        if (!Files.exists(Path.of(this.inputFileName))) {
            log.error("Input file not found: {}", this.inputFileName);
            throw new IllegalArgumentException("Input file " + this.inputFileName + " does not exist");
        }
        this.intermediateFileNamePrefix = Optional.of(intermediateFileNamePrefix)
                .orElse(String.format("%s-%d-", INTERMEDIATE_FILE_DEFAULT_NAME_PREFIX, id));
        this.id = id;
        this.reduceWorkerCount = reduceWorkerCount;
        log.info("created MapWorker: {}", this);
    }

    @Override
    public String toString() {
        return "MapWorker{" +
                "reduceWorkerCount=" + reduceWorkerCount +
                ", id=" + id +
                ", inputFileName='" + inputFileName + '\'' +
                ", intermediateFileNamePrefix='" + intermediateFileNamePrefix + '\'' +
                '}';
    }

    @Override
    public Future<Boolean> work() {
        return CompletableFuture.supplyAsync(() -> {
            List<KeyValue> kva;
            try (InputStream inputStream = MapWorker.class.getClassLoader().getResourceAsStream(inputFileName)) {
                if (inputStream == null) {
                    log.error("Input file {} does not exist", inputFileName);
                    return false;
                }
                kva = mapFunction.MapF(inputFileName, new String(inputStream.readAllBytes()));
            } catch (SecurityException | IOException e) {
                log.error("process input file {} failed {}", inputFileName, e.getMessage());
                throw new RuntimeException(e);
            }

            List<List<KeyValue>> distributedKva = new ArrayList<>(reduceWorkerCount);
            for (int i = 0; i < reduceWorkerCount; i++) {
                distributedKva.add(new ArrayList<>(kva.size() / 3));
            }
            for (KeyValue kv : kva) {
                distributedKva.get(Math.abs(kv.key().hashCode()) % reduceWorkerCount).add(kv);
            }

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
                for (String createdFile : createdFiles) {
                    File file = new File(intermediateFileNamePrefix + createdFile);
                    if (file.exists()) {
                        boolean ignore = file.delete();
                    }
                }
                return false;
            }
            return true;
        });
    }
}
