package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;

@Description("Encodes the first argument into a String using the provided character set " +
        "(one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16')")
@ScalarFunction("encode")
public class EncodeFunction {
    private EncodeFunction() {

    }

    static final List<String> lst = Arrays.asList("US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16");

    @TypeParameter("T")
    @SqlType(StandardTypes.VARBINARY)
    @SqlNullable
    public static Slice encode(@SqlType("T") Slice str, @SqlType(StandardTypes.VARCHAR) Slice charset) {
        if (!lst.contains(charset.toStringUtf8().toUpperCase())) {
            return null;
        }
        ByteBuffer b = Charset.forName(charset.toStringUtf8()).encode(str.toStringUtf8());
        String res = new String(b.array());

        return utf8Slice(res);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARBINARY)
    @SqlNullable
    public static Slice encode_long(@SqlType("T") long value, @SqlType(StandardTypes.VARCHAR) Slice charset) {
        if (!lst.contains(charset.toStringUtf8().toUpperCase())) {
            return null;
        }
        ByteBuffer b = Charset.forName(charset.toStringUtf8()).encode(Long.toString(value));
        String res = new String(b.array());

        return utf8Slice(res);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARBINARY)
    @SqlNullable
    public static Slice encode_double(@SqlType("T") double value, @SqlType(StandardTypes.VARCHAR) Slice charset) {
        if (!lst.contains(charset.toStringUtf8().toUpperCase())) {
            return null;
        }
        ByteBuffer b = Charset.forName(charset.toStringUtf8()).encode(Double.toString(value));
        String res = new String(b.array());

        return utf8Slice(res);
    }
}
