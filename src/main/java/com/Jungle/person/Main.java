package com.Jungle.person;

import com.Jungle.person.config.MapReduceConfig;
import com.Jungle.person.models.KeyValue;
import com.Jungle.person.tools.MapFunction;
import com.Jungle.person.tools.ReduceFunction;
import com.Jungle.person.tools.impl.IndexerMap;
import com.Jungle.person.tools.impl.IndexerReduce;
import com.Jungle.person.tools.impl.WcMap;
import com.Jungle.person.tools.impl.WcReduce;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class Main {

    public static void main(String[] args) throws IOException {
        log.info("Start Wc");
        MapReduce(new WcMap(), new WcReduce(), "mr-out-0");
        log.info("End Wc");

        log.info("Start Indexer");
        MapReduce(new IndexerMap(), new IndexerReduce(), "mr-out-1");
        log.info("End Indexer");
    }

    private static void MapReduce(MapFunction map_f, ReduceFunction reduce_f, String file_name) throws IOException {
        ClassLoader clr = Main.class.getClassLoader();
        List<KeyValue> intermediate = new ArrayList<>();
        for (String filename : MapReduceConfig.INPUT_FILES) {
            try (InputStream inputStream = clr.getResourceAsStream(filename)) {
                if (inputStream == null) {
                    log.error("{} not found", filename);
                    continue;
                }
                byte[] bytes = inputStream.readAllBytes();
                List<KeyValue> kva = map_f.MapF(filename, new String(bytes));
                intermediate.addAll(kva);
            }
        }
        Collections.sort(intermediate);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file_name))) {
            int i = 0;
            while (i < intermediate.size()) {
                int j = i + 1;
                while (j < intermediate.size() && intermediate.get(j).key().equals(intermediate.get(i).key())) {
                    j++;
                }
                List<String> values = new ArrayList<>();
                for (int k = i; k < j; k++) {
                    values.add(intermediate.get(k).value());
                }
                String output = reduce_f.ReduceF(intermediate.get(i).key(), values);
                writer.write(intermediate.get(i).key() + " " + output + "\n");
                i = j;
            }
        }
    }
}