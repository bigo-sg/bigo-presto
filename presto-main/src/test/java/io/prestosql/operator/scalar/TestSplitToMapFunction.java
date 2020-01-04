package io.prestosql.operator.scalar;

import io.prestosql.spi.block.MethodHandleUtil;
import io.prestosql.spi.type.MapType;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestSplitToMapFunction extends AbstractTestFunctions {

    @Test
    public void testStrToMap() {
        MapType mapType = new MapType(
                VARCHAR,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestSplitToMapFunction.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSplitToMapFunction.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSplitToMapFunction.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSplitToMapFunction.class, "throwUnsupportedOperation"));
        Map resultMap = new HashMap();
        resultMap.put("rpc_method", "sync_big_groups");
        resultMap.put("resp_type", "other");
        resultMap.put("fd", "imo");
        resultMap.put("direction", "receive");
        resultMap.put("traffic", "10256");
        assertFunction("split_to_map('rpc_method:sync_big_groups,resp_type:other,fd:imo,direction:receive,traffic:10256', ',', ':')", mapType, resultMap);
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
