package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.text.SimpleDateFormat;
import java.util.Date;

import static io.airlift.slice.Slices.utf8Slice;

public final class ExternalDateTimeFunctions
{
    private ExternalDateTimeFunctions() {}

    @ScalarFunction("from_unixtime")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime)
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long timestamp = Math.round(unixTime * 1000);
        Date date = new Date(timestamp);
        return utf8Slice(dateFormat.format(date));
    }

}
