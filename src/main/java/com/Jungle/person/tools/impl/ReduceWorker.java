package com.Jungle.person.tools.impl;

import com.Jungle.person.config.MapReduceConfig;
import com.Jungle.person.models.KeyValue;
import com.Jungle.person.tools.ReduceFunction;
import com.Jungle.person.tools.Worker;
import com.Jungle.person.util.GsonUtil;
import com.google.gson.JsonSyntaxException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @author : Jungle
 */
@Getter
@Slf4j
public class ReduceWorker implements Worker {
    private final int id;

    private final ReduceFunction reduceFunction;

    private final int mapWorkerCount;

    private final String intermediateFileNamePrefix;

    private final String outputFileNamePrefix;

    private String getIntermediateFileName(int mapWorkerId) {
        return String.format("%s%d-%d", intermediateFileNamePrefix, mapWorkerId, id);
    }

    private void checkIntermediateFiles() throws IllegalArgumentException {
        for (int i = 0; i < mapWorkerCount; i++) {
            if (!Files.exists(Path.of(MapReduceConfig.withMapWorkerTargetFolderPrefix(getIntermediateFileName(i))))) {
                throw new IllegalArgumentException("intermediate file not exist: " + MapReduceConfig.withMapWorkerTargetFolderPrefix(getIntermediateFileName(i)));
            }
        }
    }

    private void outputFileAbort(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            boolean ignore = file.delete();
        }
    }

    public ReduceWorker(int id, ReduceFunction reduceFunction, int mapWorkerCount, String intermediateFileNamePrefix, String outputFileNamePrefix) {
        this.id = id;
        this.reduceFunction = Objects.requireNonNull(reduceFunction);
        this.mapWorkerCount = mapWorkerCount;
        this.intermediateFileNamePrefix = Optional
                .ofNullable(intermediateFileNamePrefix)
                .orElse(MapReduceConfig.INTERMEDIATE_FILE_DEFAULT_NAME_PREFIX);
        this.outputFileNamePrefix = Optional
                .ofNullable(outputFileNamePrefix)
                .orElseGet(() -> String.format("%s%d", MapReduceConfig.OUTPUT_FILE_DEFAULT_NAME_PREFIX, id));
        checkIntermediateFiles();
    }

    public ReduceWorker(int id, ReduceFunction reduceFunction, int mapWorkerCount) {
        this(id, reduceFunction, mapWorkerCount, null, null);
    }

    public ReduceWorker(int id, ReduceFunction reduceFunction, int mapWorkerCount, String intermediateFileNamePrefix) {
        this(id, reduceFunction, mapWorkerCount, intermediateFileNamePrefix, null);
    }

    @Override
    public String toString() {
        return "ReduceWorker{" +
                "id=" + id +
                ", reduceFunction=" + reduceFunction.getName() +
                ", mapWorkerCount=" + mapWorkerCount +
                ", intermediateFileNamePrefix='" + intermediateFileNamePrefix + '\'' +
                ", outputFileNamePrefix='" + outputFileNamePrefix + '\'' +
                '}';
    }

    @Override
    public CompletableFuture<Boolean> work() {
        return CompletableFuture.supplyAsync(() -> {
            log.info("start reduce task:{}", this);

            List<KeyValue> kvs = new ArrayList<>();
            for (int i = 0; i < mapWorkerCount; i++) {
                String intermediateFile = getIntermediateFileName(i);
                try (InputStream inputStream = new FileInputStream(MapReduceConfig.withMapWorkerTargetFolderPrefix(intermediateFile));
                     BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        kvs.add(GsonUtil.jsonToBean(line, KeyValue.class));
                    }
                } catch (SecurityException | IOException | IllegalArgumentException | JsonSyntaxException e) {
                    log.error("process input file {} failed {}", MapReduceConfig.withMapWorkerTargetFolderPrefix(intermediateFile), e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            Collections.sort(kvs);

            boolean success = true;
            String outputFileName = MapReduceConfig.withMapWorkerTargetFolderPrefix(outputFileNamePrefix);
            try (OutputStream outputStream = new FileOutputStream(outputFileName);
                 BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
                int i = 0;
                while (i < kvs.size()) {
                    int j = i + 1;
                    while (j < kvs.size() && kvs.get(i).key().equals(kvs.get(j).key())) {
                        j++;
                    }
                    List<String> values = new ArrayList<>(j - i);
                    for (int k = i; k < j; k++) {
                        values.add(kvs.get(k).value());
                    }
                    String output = reduceFunction.ReduceF(kvs.get(i).key(), values);
                    bufferedWriter.write(new KeyValue(kvs.get(i).key(), output).toJsonLine());
                    i = j;
                }
            } catch (IOException e) {
                log.error("process output file:{} fail:{}", outputFileName, e.getMessage());
                success = false;
            }

            if (!success) {
                outputFileAbort(outputFileName);
                return false;
            }

            return true;
        });
    }
}
