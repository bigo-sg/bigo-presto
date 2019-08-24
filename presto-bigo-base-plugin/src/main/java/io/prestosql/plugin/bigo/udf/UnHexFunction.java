package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import static io.airlift.slice.Slices.utf8Slice;

public class UnHexFunction {

    @Description("Hex number to binary.")
    @ScalarFunction("unhex")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice hexToBinary(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return utf8Slice(evaluateUnhex(slice.toStringUtf8()));
    }

    protected static String evaluateUnhex(String s) {
        if (s == null) {
            return null;
        }

        String str;
        if (s.length() % 2 == 1) {
            str = "0" + s;
        } else {
            str = s;
        }

        byte[] result = new byte[str.length() / 2];
        for (int i = 0; i < str.length(); i += 2) {
            try {
                result[i / 2] = ((byte) Integer.parseInt(str.substring(i, i + 2), 16));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        return new String(result);
    }
}
