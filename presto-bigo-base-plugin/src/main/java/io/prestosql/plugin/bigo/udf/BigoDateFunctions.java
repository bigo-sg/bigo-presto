package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class BigoDateFunctions {
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    @Description("Returns the date that is num_days after start_date.")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateAdd(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long daysToAdd)
    {
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(formatter.parse(startDate.toStringUtf8()));
        } catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        c.add(Calendar.DATE, (int)daysToAdd);

        return utf8Slice(formatter.format(c.getTime()));
    }

    @Description("Returns the date that is num_days before start_date.")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateSub(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long daysToAdd)
    {
        return dateAdd(startDate, -daysToAdd);
    }
}
