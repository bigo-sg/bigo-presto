package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.BytesWritable;

public class BigoIsAVTest
{

    @Description("self build hive udf, isavtest")
    @ScalarFunction("isavtest")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isAvTest(@SqlType(StandardTypes.INTEGER) long id,
                                   @SqlType(StandardTypes.VARCHAR) Slice target,
                                   @SqlType(StandardTypes.INTEGER) long abFirst)
    {
//        return isAVTestOn((int) id, target.toStringUtf8(), (int) abFirst);
//        return isAVTestOn((int) id, valueUnBase64(target.toStringUtf8()), (int) abFirst);
        if (target == null) {
            return false;
        }
        byte[] bytes = new byte[target.length()];
        System.arraycopy(target.getBytes(), 0, bytes, 0, target.length());
        byte[] decoded = Base64.decodeBase64(bytes);
        BytesWritable result = new BytesWritable();
        result.set(decoded, 0, decoded.length);

//        return isAVTestOn((int)id, decoded, (int)abFirst);
        return isAVTestOn((int)id, result.getBytes(), (int)abFirst);
    }

    @Description("self build hive udf, isavtest")
    @ScalarFunction("isavtest")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isAvTestLong(@SqlType(StandardTypes.BIGINT) long id,
                                       @SqlType(StandardTypes.VARCHAR) Slice target,
                                       @SqlType(StandardTypes.BIGINT) long abFirst)
    {
//        return isAVTestOn((int) id, valueUnBase64(target.toStringUtf8()), (int) abFirst);
        if (target == null) {
            return false;
        }
        byte[] bytes = new byte[target.length()];
        System.arraycopy(target.getBytes(), 0, bytes, 0, target.length());
        byte[] decoded = Base64.decodeBase64(bytes);
        BytesWritable result = new BytesWritable();
        result.set(decoded, 0, decoded.length);

        return isAVTestOn((int)id, result.getBytes(), (int)abFirst);
//        return isAVTestOn((int)id, decoded, (int)abFirst);
    }

    @Description("self build hive udf, isavtest")
    @ScalarFunction("isavtest")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isAvTestByteInt(@SqlType(StandardTypes.INTEGER) long id,
                                       @SqlType(StandardTypes.VARBINARY) Slice target,
                                       @SqlType(StandardTypes.INTEGER) long abFirst)
    {
        return isAVTestOn((int) id, target.getBytes(), (int) abFirst);
    }

    @Description("self build hive udf, isavtest")
    @ScalarFunction("isavtest")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isAvTestByteLong(@SqlType(StandardTypes.BIGINT) long id,
                                       @SqlType(StandardTypes.VARBINARY) Slice target,
                                       @SqlType(StandardTypes.BIGINT) long abFirst)
    {
        return isAVTestOn((int) id, target.getBytes(), (int) abFirst);
    }

    public static boolean isAVTestOn(int id, byte[] abVector, int abFirst)
    {
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

//    protected static boolean isAVTestOn(int id, String target, int abFirst)
//    {
//        byte[] abVector = target.getBytes();
//        id = id - abFirst;
//        if (id >= 0 && abVector != null) {
//            int B = id / 8;
//            int b = id % 8;
//            if (B >= 0 && B < abVector.length) {
//                return (abVector[abVector.length - 1 - B] & (1 << b)) != 0;
//            }
//        }
//        return false;
//    }

//    private static boolean evaluate(int id, byte[] abVector, int abFirst)
//    {
//        return BigoIsAVTest.isAVTestOn(id, abVector, abFirst);
//    }

    private static byte[] valueUnBase64(String value){
        if (value == null) {
            return null;
        }
        byte[] bytes = new byte[value.length()];
        System.arraycopy(value.getBytes(), 0, bytes, 0, value.length());

        return Base64.decodeBase64(value.getBytes());
    }
}
