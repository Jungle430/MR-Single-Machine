package com.Jungle.person;

import com.Jungle.person.config.MapReduceConfig;
import com.Jungle.person.tools.impl.IndexerMap;
import com.Jungle.person.tools.impl.IndexerReduce;
import com.Jungle.person.tools.impl.WcMap;
import com.Jungle.person.tools.impl.WcReduce;
import com.Jungle.person.util.MapReduceUtil;

/**
 * @author Jungle
 */
public class Main {

    public static void main(String[] args) {
        MapReduceUtil.MapReduce(MapReduceConfig.INPUT_FILES, new WcMap(), new WcReduce(), "WC", "WC");
        MapReduceUtil.MapReduce(MapReduceConfig.INPUT_FILES, new IndexerMap(), new IndexerReduce());
    }

}