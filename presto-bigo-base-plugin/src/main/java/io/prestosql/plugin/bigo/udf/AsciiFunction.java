package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public class AsciiFunction {

    @Description("Returns the numeric value of the first character of str.")
    @ScalarFunction("ascii")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long ascii(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        if (slice == null || slice.toStringUtf8().length()==0) {
            return null;
        }
        return (long)slice.toStringUtf8().charAt(0);
    }
}
