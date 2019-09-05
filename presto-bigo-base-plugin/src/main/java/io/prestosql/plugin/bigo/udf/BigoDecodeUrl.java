package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.net.URLDecoder;

import static io.airlift.slice.Slices.utf8Slice;

public class BigoDecodeUrl {

    @Description("self build hive udf, decodeurl")
    @ScalarFunction("decodeurl")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice decodeUrl(@SqlType(StandardTypes.VARCHAR) Slice str) {
        return utf8Slice(evaluate(str.toStringUtf8()));
    }

    public static String evaluate(String input) {
        //return null if input is null
        if (input == null)
            return null;

        //return decoded string.  if exception return null
        // decodec twice for complete decode
        try {
            return URLDecoder.decode(URLDecoder.decode(input, "UTF-8"), "UTF-8");
        } catch (Exception e) {
            return null;
        }
    }
}