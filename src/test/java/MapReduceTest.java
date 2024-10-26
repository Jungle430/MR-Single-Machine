import com.Jungle.person.config.MapReduceConfig;
import com.Jungle.person.tools.impl.IndexerMap;
import com.Jungle.person.tools.impl.IndexerReduce;
import com.Jungle.person.tools.impl.WcMap;
import com.Jungle.person.tools.impl.WcReduce;
import com.Jungle.person.util.MapReduceUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * @author Jungle
 */
public class MapReduceTest {

    boolean shouldTestMapReduce() {
        return MapReduceConfig.MAPREDUCE_TEST;
    }

    @Test
    @EnabledIf("shouldTestMapReduce")
    public void TestMapReduce1() {
        MapReduceUtil.MapReduce(MapReduceConfig.INPUT_FILES, new WcMap(), new WcReduce());
    }


    @Test
    @EnabledIf("shouldTestMapReduce")
    public void TestMapReduce2() {
        MapReduceUtil.MapReduce(MapReduceConfig.INPUT_FILES, new IndexerMap(), new IndexerReduce(), "TEST_TEMP");
    }

    @Test
    @EnabledIf("shouldTestMapReduce")
    public void TestMapReduce3() {
        MapReduceUtil.MapReduce(MapReduceConfig.INPUT_FILES, new IndexerMap(), new IndexerReduce(), "TEST_TEMP", "TEST_TEMP");
    }
}
