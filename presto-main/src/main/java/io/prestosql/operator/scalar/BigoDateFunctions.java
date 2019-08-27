package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

public final class BigoDateFunctions
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    private BigoDateFunctions() {}

    @ScalarFunction("to_date")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice toDateWithTimeZone(
            ConnectorSession session,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        Slice dateString;
        if (session.isLegacyTimestamp()) {
            DateTimeFormatter formatter = ISODateTimeFormat.dateTime()
                    .withChronology(getChronology(session.getTimeZoneKey()));
            dateString = utf8Slice(formatter.print(timestamp));
        }
        else {
            DateTimeFormatter formatter = ISODateTimeFormat.dateHourMinuteSecondMillis()
                    .withChronology(UTC_CHRONOLOGY);
            dateString = utf8Slice(formatter.print(timestamp));
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            java.sql.Date date = new java.sql.Date(0);
            Date parsedVal = formatter.parse(dateString.toStringUtf8());
            date.setTime(parsedVal.getTime());
            return utf8Slice(date.toString());
        } catch (ParseException e) {
            return null;
        }
    }

    @ScalarFunction("to_date")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice toDateWithTimeStamp(
            ConnectorSession session,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        Slice dateString;
        if (session.isLegacyTimestamp()) {
            DateTimeFormatter formatter = ISODateTimeFormat.dateTime()
                    .withChronology(getChronology(session.getTimeZoneKey()));
            dateString = utf8Slice(formatter.print(timestamp));
        }
        else {
            DateTimeFormatter formatter = ISODateTimeFormat.dateHourMinuteSecondMillis()
                    .withChronology(UTC_CHRONOLOGY);
            dateString = utf8Slice(formatter.print(timestamp));
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            java.sql.Date date = new java.sql.Date(0);
            Date parsedVal = formatter.parse(dateString.toStringUtf8());
            date.setTime(parsedVal.getTime());
            return utf8Slice(date.toString());
        } catch (ParseException e) {
            return null;
        }
    }
}
