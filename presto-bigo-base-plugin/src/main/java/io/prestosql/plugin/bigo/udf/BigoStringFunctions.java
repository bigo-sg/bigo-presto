package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import sun.util.resources.ga.LocaleNames_ga;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;

public class BigoStringFunctions {

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long inStr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        if (substring.length() == 0) {
            return 1;
        }

        int index = string.indexOf(substring);
        if (index < 0) {
            return 0;
        }
        return countCodePoints(string, 0, index) + 1;
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long inStr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.INTEGER) long substring)
    {
        String str = Long.toString(substring);
        return inStr(string, utf8Slice(str));
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long inStr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.DOUBLE) double substring)
    {
        String str = Double.toString(substring);
        return inStr(string, utf8Slice(str));
    }

}
