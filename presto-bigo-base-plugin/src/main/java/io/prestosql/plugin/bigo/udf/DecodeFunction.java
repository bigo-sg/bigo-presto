package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;

public class DecodeFunction {

    @Description("Decodes the first argument into a String using the provided character set " +
            "(one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16')")
    @ScalarFunction("decode")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice decode(@SqlType(StandardTypes.VARBINARY) Slice str, @SqlType(StandardTypes.VARCHAR) Slice charset) {
        List<String> lst = Arrays.asList("US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16");
        if (!lst.contains(charset.toStringUtf8().toUpperCase())) {
            return null;
        }
        String res = Charset.forName(charset.toStringUtf8()).decode(str.toByteBuffer()).toString();
        return utf8Slice(res);
    }
}