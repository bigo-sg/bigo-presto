package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public class BigoIsAVTest {

    @Description("self build hive udf, isavtest")
    @ScalarFunction("isavtest")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isAvTest(@SqlType(StandardTypes.INTEGER) long id,
                                   @SqlType(StandardTypes.VARCHAR) Slice target,
                                   @SqlType(StandardTypes.INTEGER) long abFirst) {
        return evaluate((int) id, target.toStringUtf8(), (int) abFirst);
    }

    @Description("self build hive udf, isavtest")
    @ScalarFunction("isavtest")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isAvTestLong(@SqlType(StandardTypes.BIGINT) long id,
                                       @SqlType(StandardTypes.VARCHAR) Slice target,
                                       @SqlType(StandardTypes.BIGINT) long abFirst) {
        return evaluate((int) id, target.toStringUtf8(), (int) abFirst);
    }

    public static boolean isAVTestOn(int id, String target, int abFirst) {
        byte[] abVector = target.getBytes();
        id -= abFirst;
        if (id >= 0 && abVector != null) {
            int B = id / 8;
            int b = id % 8;
            if (B >= 0 && B < abVector.length) {
                return (abVector[abVector.length - 1 - B] & (1 << b)) != 0;
            }
        }
        return false;
    }

    public static boolean evaluate(int id, String target, int abFirst) {
        return BigoIsAVTest.isAVTestOn(id, target, abFirst);
    }
}
