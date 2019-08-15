package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public class BigoConditionalFunctions {

    @Description("Returns default value if value is null else returns value.")
    @ScalarFunction("nvl")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice nvlString(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value,
                            @SqlType(StandardTypes.VARCHAR) Slice default_value)
    {
        if (value == null) {
            return default_value;
        } else {
            return value;
        }
    }

    @Description("Returns default value if value is null else returns value.")
    @ScalarFunction("nvl")
    @SqlType(StandardTypes.INTEGER)
    public static long nvlInt(@SqlNullable @SqlType(StandardTypes.INTEGER) Long value,
                           @SqlType(StandardTypes.INTEGER) long default_value)
    {
        if (value == null) {
            return default_value;
        } else {
            return value;
        }
    }

    @Description("Returns default value if value is null else returns value.")
    @ScalarFunction("nvl")
    @SqlType(StandardTypes.DOUBLE)
    public static double nvlDouble(@SqlNullable @SqlType(StandardTypes.DOUBLE) Double value,
                             @SqlType(StandardTypes.DOUBLE) double default_value)
    {
        if (value == null) {
            return default_value;
        } else {
            return value;
        }
    }

    @Description("Returns default value if value is null else returns value.")
    @ScalarFunction("nvl")
    @SqlType(StandardTypes.DATE)
    public static long nvlDate(@SqlNullable @SqlType(StandardTypes.DATE) Long value,
                           @SqlType(StandardTypes.DATE) long default_value)
    {
        if (value == null) {
            return default_value;
        } else {
            return value;
        }
    }

    @Description("Returns default value if value is null else returns value.")
    @ScalarFunction("nvl")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long nvlTimeStamp(@SqlNullable @SqlType(StandardTypes.TIMESTAMP) Long value,
                                @SqlType(StandardTypes.TIMESTAMP) long default_value)
    {
        if (value == null) {
            return default_value;
        } else {
            return value;
        }
    }

    @Description("Returns default value if value is null else returns value.")
    @ScalarFunction("nvl")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long nvlTimeStampWithTimeZone(@SqlNullable @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) Long value,
                                     @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long default_value)
    {
        if (value == null) {
            return default_value;
        } else {
            return value;
        }
    }
}
