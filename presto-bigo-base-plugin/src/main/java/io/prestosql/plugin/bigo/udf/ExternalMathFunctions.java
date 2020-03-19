package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.StandardTypes;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public final class ExternalMathFunctions {

    private ExternalMathFunctions() {}

    @Description("convert a string in the given base to a number")
    @ScalarFunction("conv")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice conv(@SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.BIGINT) long from, @SqlType(StandardTypes.BIGINT) long to)
    {
        if (null == value) {
            return null;
        }
        if (from < 2 || from > 36 || to < 2 || to > 36) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Not a valid base number from %d to %d with %s", from, to, value.toStringUtf8()));
        }

        try {
            String result = change(value.toStringUtf8(), from, to);
            return utf8Slice(result);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Not a valid base number from %d to %d with %s", from, to, value.toStringUtf8()), e);
        }
    }

    private static String change(String number, long from, long to) {
        return new BigInteger(number, (int) from).toString((int) to);
    }

    @Description("Returns a string representation of the binary value of N, where N is a longlong (BIGINT) number. This is equivalent to CONV(N,10,2). Returns NULL if N is NULL.")
    @ScalarFunction("bin")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice bin(@SqlType(StandardTypes.BIGINT) long value) {
        return conv(utf8Slice(String.valueOf(value)), 10, 2);
    }

}
