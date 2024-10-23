import com.Jungle.person.models.KeyValue;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class KeyValueJsonTest {
    private static final int TEST_TO_JSON_METHOD_COUNT = 200;

    @Test
    public void testToJsonMethod() {
        List<KeyValue> keyValues = new ArrayList<>();
        List<String> jsonKVs = new ArrayList<>();

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            keyValues.add(new KeyValue("key" + i, "value" + i));
            jsonKVs.add(String.format("{\"key\":\"key%d\",\"value\":\"value%d\"}", i, i));
        }

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            Assert.assertEquals(keyValues.get(i).toJson(), jsonKVs.get(i));
        }
    }
}
