import com.Jungle.person.models.KeyValue;
import com.Jungle.person.util.GsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jungle
 */
public class KeyValueJsonTest {
    private static final int TEST_TO_JSON_METHOD_COUNT = 200;

    @Test
    public void TestKeyValueToJson() {
        List<KeyValue> keyValues = new ArrayList<>();
        List<String> jsonKVs = new ArrayList<>();

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            keyValues.add(new KeyValue("key" + i, "value" + i));
            jsonKVs.add(String.format("{\"key\":\"key%d\",\"value\":\"value%d\"}", i, i));
        }

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            Assertions.assertEquals(keyValues.get(i).toJson(), jsonKVs.get(i));
        }
    }

    @Test
    public void TestKeyValueToJsonLine() {
        List<KeyValue> keyValues = new ArrayList<>();
        List<String> jsonKVs = new ArrayList<>();

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            keyValues.add(new KeyValue("key" + i, "value" + i));
            jsonKVs.add(String.format("{\"key\":\"key%d\",\"value\":\"value%d\"}", i, i));
        }

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            Assertions.assertEquals(keyValues.get(i).toJsonLine(), jsonKVs.get(i) + '\n');
        }
    }

    @Test
    public void TestBeanToJsonMethod() {
        List<KeyValue> keyValues = new ArrayList<>();
        List<String> jsonKVs = new ArrayList<>();

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            keyValues.add(new KeyValue("key" + i, "value" + i));
            jsonKVs.add(String.format("{\"key\":\"key%d\",\"value\":\"value%d\"}", i, i));
        }

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            Assertions.assertEquals(GsonUtil.beanToJson(keyValues.get(i)), jsonKVs.get(i));
        }
    }

    @Test
    public void TestJsonToBeanMethod() {
        List<KeyValue> keyValues = new ArrayList<>();
        List<String> jsonKVs = new ArrayList<>();
        List<String> jsonKVsWithLine = new ArrayList<>();

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            keyValues.add(new KeyValue("key" + i, "value" + i));
            jsonKVs.add(String.format("{\"key\":\"key%d\",\"value\":\"value%d\"}", i, i));
            jsonKVsWithLine.add(String.format("{\"key\":\"key%d\",\"value\":\"value%d\"}\n", i, i));
        }

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            Assertions.assertEquals(keyValues.get(i), GsonUtil.jsonToBean(jsonKVs.get(i), KeyValue.class));
        }

        for (int i = 0; i < TEST_TO_JSON_METHOD_COUNT; i++) {
            Assertions.assertEquals(keyValues.get(i), GsonUtil.jsonToBean(jsonKVsWithLine.get(i), KeyValue.class));
        }
    }
}
