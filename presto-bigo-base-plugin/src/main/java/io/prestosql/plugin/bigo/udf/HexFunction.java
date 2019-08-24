package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import java.util.Base64;

import static io.airlift.slice.Slices.utf8Slice;

public class HexFunction {

    @Description("Returns hex value from integer number.")
    @ScalarFunction("hex")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice intToHex(@SqlType(StandardTypes.BIGINT) long num)
    {
        return utf8Slice(evaluate(num));
    }

    @Description("Returns hex value from string number.")
    @ScalarFunction("hex")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stringToHex(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return utf8Slice(evaluate(slice.toStringUtf8()));
    }

    @Description("Returns hex value from binary.")
    @ScalarFunction("hex")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice binaryToHex(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return Slices.wrappedBuffer(Base64.getUrlEncoder().encode(slice.getBytes()));
    }

    protected static String evaluate(long num) {
        byte[] value = new byte[16];
        int len = 0;
        do {
            len++;
            value[value.length - len] = (byte) Character.toUpperCase(Character
                    .forDigit((int) (num & 0xF), 16));
            num >>>= 4;
        } while (num != 0);

        byte[] res = new byte[len];
        System.arraycopy(value, value.length - len, res, 0, len);
        return new String(res);
    }

    protected static String evaluate(String s) {
        if (s == null) {
            return null;
        }

        byte[] str = s.getBytes();
        return evaluate(str, s.length());
    }

    protected static String evaluate(byte[] bytes, int length){
        byte[] value = new byte[16];
        if (value.length < length * 2) {
            value = new byte[length * 2];
        }

        for (int i = 0; i < length; i++) {
            value[i * 2] = (byte) Character.toUpperCase(Character.forDigit(
                    (bytes[i] & 0xF0) >>> 4, 16));
            value[i * 2 + 1] = (byte) Character.toUpperCase(Character.forDigit(
                    bytes[i] & 0x0F, 16));
        }
        byte[] res = new byte[length*2];
        System.arraycopy(value, 0, res, 0, length * 2);
        return new String(res);

    }
}
