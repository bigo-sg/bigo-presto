/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import io.airlift.log.Logger;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.bigo.util.DateTimeZoneIndexUtil.getChronology;
import static io.prestosql.plugin.bigo.util.DateTimeZoneIndexUtil.unpackChronology;
import static io.prestosql.plugin.bigo.util.QuarterOfYearDateTimeField.QUARTER_OF_YEAR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.updateMillisUtc;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BigoDateTimeFunctions {
    private static final Logger LOG = Logger.get(BigoDateTimeFunctions.class);
    private static final String dateFormat1 = "yyyy-MM-dd HH:mm:ss";
    private static final String dateFormat2 = "yyyy-MM-dd";
    private static final String dateFormat3 = "HH:mm:ss";
    private static final String TIME_ZONE_LOCAL = "Asia/Shanghai";
    private static final ISOChronology CHRONOLOGY = ISOChronology.getInstance(DateTimeZone.forTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL)));
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    @Description("Returns the date that is num_days after start_date.")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateAdd(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long daysToAdd) {
        // create SimpleDateFormat in every call as it's not thread safe.
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat2);

        Calendar c = Calendar.getInstance();
        try {
            c.setTime(formatter.parse(startDate.toStringUtf8()));
        } catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        c.add(Calendar.DATE, (int) daysToAdd);

        return utf8Slice(formatter.format(c.getTime()));
    }

    @Description("Returns the date that is num_days before start_date.")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateSub(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long daysToAdd) {
        return dateAdd(startDate, -daysToAdd);
    }

    @ScalarFunction("toUnixTime")
    @SqlType(StandardTypes.BIGINT)
    public static long toUnixTime(@SqlType(StandardTypes.TIMESTAMP) long timestamp) {
        return timestamp / 1000;
    }

    @Description("Gets current UNIX timestamp in seconds")
    @ScalarFunction("unix_timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long currentUnixTimestamp(ConnectorSession session) {
        return session.getStartTime() / 1000;
    }

    @ScalarFunction("unix_timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long unixTimestamp(@SqlType(StandardTypes.VARCHAR) Slice sliceTime) {
        SimpleDateFormat df = new SimpleDateFormat(dateFormat1);
        df.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL));
        try {
            Date date = df.parse(sliceTime.toStringUtf8());
            return toUnixTime(date.getTime());
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return 0;
    }

    @ScalarFunction("unix_timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long unixTimestamp(@SqlType(StandardTypes.VARCHAR) Slice sliceTime, @SqlType(StandardTypes.VARCHAR) Slice sliceFormat) {
        SimpleDateFormat df = new SimpleDateFormat(sliceFormat.toStringUtf8());
        df.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL));
        try {
            Date date = df.parse(sliceTime.toStringUtf8());
            return toUnixTime(date.getTime());
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return 0;
    }

    @Description("Converts unixtime to a string representing the timestamp according to the given format.")
    @ScalarFunction("from_unixtime")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUnixtime(@SqlType(StandardTypes.BIGINT) long time, @SqlType(StandardTypes.VARCHAR) Slice format) throws ParseException {
        String message = "";
        try {
            ZonedDateTime timestamp = ZonedDateTime.of(LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.ofHours(8)), ZoneId.of("UTC"));
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.toStringUtf8());
            return utf8Slice(timestamp.format(formatter));
        } catch (Exception e) {
            message = e.getMessage();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, message);
    }

    @Description("Converts unixtime to a string representing the timestamp according to the given format.")
    @ScalarFunction("from_utc_timestamp")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtcTimestamp(@SqlType(StandardTypes.BIGINT) long time, @SqlType(StandardTypes.VARCHAR) Slice format) throws ParseException {
        String message = "";
        try {
            ZonedDateTime timestamp = ZonedDateTime.of(LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.UTC), ZoneId.of("UTC"));
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.toStringUtf8());
            return utf8Slice(timestamp.format(formatter));
        } catch (Exception e) {
            message = e.getMessage();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, message);
    }

    @Description("Returns the date that is num_days after start_date.")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long dateDiff(
            @SqlType(StandardTypes.VARCHAR) Slice endDate,
            @SqlType(StandardTypes.VARCHAR) Slice startDate) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat2);

        Calendar cEnd = Calendar.getInstance();
        Calendar cStart = Calendar.getInstance();
        try {
            cEnd.setTime(formatter.parse(endDate.toStringUtf8()));
            cStart.setTime(formatter.parse(startDate.toStringUtf8()));
        } catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        return TimeUnit.MILLISECONDS.toDays(cEnd.getTimeInMillis() - cStart.getTimeInMillis());
    }

    @Description("hour of the given string")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromString(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat3);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.HOUR_OF_DAY);
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("minute of the given string")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromString(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat3);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.MINUTE);
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("second of the given string")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromString(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat3);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.SECOND);
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("day of the given string")
    @ScalarFunction("day")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromString(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.DAY_OF_MONTH);
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("month of the given string")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromString(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.MONTH) + 1;
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("year of the given string")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromString(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.YEAR);
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("weekofyear of the given string")
    @ScalarFunction("weekofyear")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfYear(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.WEEK_OF_YEAR);
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("weekofyear of the given string")
    @ScalarFunction("weekofmonth")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfMonth(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            return calendar.get(Calendar.WEEK_OF_MONTH);
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("quarter of the given string")
    @ScalarFunction("quarter")
    @SqlType(StandardTypes.BIGINT)
    public static long quarterFromString(@SqlType(StandardTypes.VARCHAR) Slice sliceDate) {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        try {
            Date date = null;
            try {
                date = formatter1.parse(sliceDate.toStringUtf8());
            } catch (ParseException e) {
                date = formatter2.parse(sliceDate.toStringUtf8());
            }
            calendar.setTime(date);
            int month = calendar.get(Calendar.MONTH);
            return month / 3 + 1;
        } catch (ParseException e) {
            return -1;
        }
    }

    @Description("Returns the date part of the timestamp string")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice stringToDate(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            java.sql.Date date = new java.sql.Date(0);
            Date parsedVal = formatter.parse(inputTimestamp.toStringUtf8());
            date.setTime(parsedVal.getTime());
            return utf8Slice(date.toString());
        } catch (ParseException e) {
            return null;
        }
    }

    @Description("cal period time.")
    @ScalarFunction("cal_pt")
    @SqlType(StandardTypes.DOUBLE)
    public static double cal_pt(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        String str = slice.toStringUtf8().toUpperCase();
        int pIndex = str.indexOf("PT");
        int mIndex = str.indexOf("M");
        int sIndex = str.indexOf("S");
        int hIndex = str.indexOf("H");
        double res = 0;

        Pattern pattern1 = Pattern.compile("[PT].*[0-9.].*[H].*[0-9.].*[M].*[0-9.].*[S]");
        Pattern pattern2 = Pattern.compile("[PT].*[0-9.].*[H].*[0-9.].*[S]");
        Pattern pattern3 = Pattern.compile("[PT].*[0-9.].*[M].*[0-9.].*[S]");
        Pattern pattern4 = Pattern.compile("[PT].*[0-9.].*[S]");

        try {
            if (pattern1.matcher(str).matches()) {
                res = Double.parseDouble(str.substring(pIndex + 2, hIndex)) * 60 * 60
                        + Double.parseDouble(str.substring(hIndex + 1, mIndex)) * 60
                        + Double.parseDouble(str.substring(mIndex + 1, sIndex));
            } else if (pattern2.matcher(str).matches()) {
                res = Double.parseDouble(str.substring(pIndex + 2, hIndex)) * 3600
                        + Double.parseDouble(str.substring(hIndex + 1, sIndex));
            } else if (pattern3.matcher(str).matches()) {
                res = Double.parseDouble(str.substring(pIndex + 2, mIndex)) * 60
                        + Double.parseDouble(str.substring(mIndex + 1, sIndex));
            } else if (pattern4.matcher(str).matches()) {
                res += Double.parseDouble(str.substring(pIndex + 2, sIndex));
            }
        } catch (NumberFormatException e) {
            return -1;
        }
        return res;
    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonths(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(formatter.parse(startDate.toStringUtf8()));
        } catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        calendar.add(Calendar.MONTH, (int) monthsToAdd);
        return utf8Slice(formatter.format(calendar.getTime()));
    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonthsDate(
            @SqlType(StandardTypes.DATE) long startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat2);
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(new Date(TimeUnit.DAYS.toMillis(startDate)));
        calendar.add(Calendar.MONTH, (int) monthsToAdd);

        return utf8Slice(formatter.format(calendar.getTime()));
    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonthsTimeStamp(
            @SqlType(StandardTypes.TIMESTAMP) long startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        Timestamp ts = new Timestamp(startDate);
        calendar.setTimeInMillis(ts.getTime());
        calendar.add(Calendar.MONTH, (int) monthsToAdd);

        return utf8Slice(formatter.format(calendar.getTime()));
    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonthsTimeStampWithZone(
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat2);

        Calendar calendar = Calendar.getInstance();
        Timestamp ts = new Timestamp(startDate);
        calendar.setTimeInMillis(ts.getTime());
        calendar.add(Calendar.MONTH, (int) monthsToAdd);

        return utf8Slice(formatter.format(calendar.getTime()));
    }

    private static Slice timeStampToDate(ConnectorSession session, long timestamp, String pattern) {
        Slice dateString;
        if (session.isLegacyTimestamp()) {
            org.joda.time.format.DateTimeFormatter formatter = ISODateTimeFormat.dateTime()
                    .withChronology(getChronology(session.getTimeZoneKey()));
            dateString = utf8Slice(formatter.print(timestamp));
        } else {
            org.joda.time.format.DateTimeFormatter formatter = ISODateTimeFormat.dateHourMinuteSecondMillis()
                    .withChronology(CHRONOLOGY);
            dateString = utf8Slice(formatter.print(timestamp));
        }
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        try {
            Date date = new Date(0);
            Date parsedVal = formatter.parse(dateString.toStringUtf8());
            date.setTime(parsedVal.getTime());
            return utf8Slice(date.toString());
        } catch (ParseException e) {
            return null;
        }
    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonths(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd,
            @SqlType(StandardTypes.VARCHAR) Slice pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern.toStringUtf8());

        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(formatter.parse(startDate.toStringUtf8()));
        } catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        calendar.add(Calendar.MONTH, (int) monthsToAdd);

        return utf8Slice(formatter.format(calendar.getTime()));
    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonthsDate(
            @SqlType(StandardTypes.DATE) long startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd,
            @SqlType(StandardTypes.VARCHAR) Slice pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern.toStringUtf8());
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(new Date(TimeUnit.DAYS.toMillis(startDate)));
        calendar.add(Calendar.MONTH, (int) monthsToAdd);

        return utf8Slice(formatter.format(calendar.getTime()));

    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonthsTimeStamp(
            @SqlType(StandardTypes.TIMESTAMP) long startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd,
            @SqlType(StandardTypes.VARCHAR) Slice pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern.toStringUtf8());

        Calendar calendar = Calendar.getInstance();
        Timestamp ts = new Timestamp(startDate);
        calendar.setTimeInMillis(ts.getTime());
        calendar.add(Calendar.MONTH, (int) monthsToAdd);

        return utf8Slice(formatter.format(calendar.getTime()));
    }

    @Description("Returns the date that is monthsToAdd after start_date.")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonthsTimeStampWithZone(
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startDate,
            @SqlType(StandardTypes.INTEGER) long monthsToAdd,
            @SqlType(StandardTypes.VARCHAR) Slice pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern.toStringUtf8());

        Calendar calendar = Calendar.getInstance();
        Timestamp ts = new Timestamp(startDate);
        calendar.setTimeInMillis(ts.getTime());
        calendar.add(Calendar.MONTH, (int) monthsToAdd);

        return utf8Slice(formatter.format(calendar.getTime()));
    }


    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long truncateDate(@SqlType(StandardTypes.DATE) long date, @SqlType("varchar(x)") Slice unit)
    {
        long millis = getDateField(CHRONOLOGY, unit).roundFloor(DAYS.toMillis(date));
        return MILLISECONDS.toDays(millis);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME)
    public static long truncateTime(ConnectorSession session, @SqlType(StandardTypes.TIME) long time, @SqlType("varchar(x)") Slice unit)
    {
        if (session.isLegacyTimestamp()) {
            return getTimeField(getChronology(session.getTimeZoneKey()), unit).roundFloor(time);
        }
        else {
            return getTimeField(CHRONOLOGY, unit).roundFloor(time);
        }
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long truncateTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone, @SqlType("varchar(x)") Slice unit)
    {
        long millis = getTimeField(unpackChronology(timeWithTimeZone), unit).roundFloor(unpackMillisUtc(timeWithTimeZone));
        return updateMillisUtc(millis, timeWithTimeZone);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long truncateTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice unit)
    {
        if (session.isLegacyTimestamp()) {
            return getTimestampField(getChronology(session.getTimeZoneKey()), unit).roundFloor(timestamp);
        }
        else {
            return getTimestampField(CHRONOLOGY, unit).roundFloor(timestamp);
        }
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long truncateTimestampWithTimezone( @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone, @SqlType("varchar(x)") Slice unit)
    {
        long millis = getTimestampField(unpackChronology(timestampWithTimeZone), unit).roundFloor(unpackMillisUtc(timestampWithTimeZone));
        return updateMillisUtc(millis, timestampWithTimeZone);
    }

    @ScalarFunction("trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice truncateVarchar(ConnectorSession connectorSession, @SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.VARCHAR) Slice unit)
    {
        SimpleDateFormat formatter1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat formatter2 = new SimpleDateFormat(dateFormat2);
        formatter1.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL));
        formatter2.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL));

        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(formatter1.parse(slice.toStringUtf8()));
        } catch (ParseException e) {
            try {
                calendar.setTime(formatter2.parse(slice.toStringUtf8()));
            } catch (ParseException ex) {
                return null;
            }
        }
        return formatDatetime(truncateTimestamp(connectorSession, calendar.getTimeInMillis(), unit));
    }

    private static DateTimeField getDateField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid DATE field");
    }

    private static DateTimeField getTimeField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return chronology.millisOfSecond();
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Time field");
    }

    private static DateTimeField getTimestampField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return chronology.millisOfSecond();
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
            case "mon":
            case "mm":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
            case "yyyy":
            case "yy":
                return chronology.year();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Timestamp field");
    }

    private static Slice formatDatetime(long timestamp)
    {
        try {
            return utf8Slice(DateTimeFormat.forPattern(BigoDateTimeFunctions.dateFormat2)
                    .withChronology(CHRONOLOGY)
                    .print(timestamp));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction("date_format")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice dateFormat(
            @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.VARCHAR) Slice format) {
        if (slice == null || format == null) {
            return null;
        }

        SimpleDateFormat df1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat df2 = new SimpleDateFormat(dateFormat2);
        df1.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL));
        df2.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL));

        SimpleDateFormat fm = new SimpleDateFormat(format.toStringUtf8());
        fm.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_LOCAL));

        Date date;
        try {
            date = df1.parse(slice.toStringUtf8());
        } catch (Exception e) {
            try {
                date = df2.parse(slice.toStringUtf8());
            } catch (ParseException ex) {
                return null;
            }
        }

        return utf8Slice(fm.format(date));
    }

    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice toUTCTimestamp(
            @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.VARCHAR) Slice timezone) {
        if (slice == null || timezone == null) {
            return null;
        }
        SimpleDateFormat df1 = new SimpleDateFormat(dateFormat1);
        SimpleDateFormat df2 = new SimpleDateFormat(dateFormat2);
        df1.setTimeZone(TimeZone.getTimeZone(timezone.toStringUtf8()));
        df2.setTimeZone(TimeZone.getTimeZone(timezone.toStringUtf8()));

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat1);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            Date date = df1.parse(slice.toStringUtf8());
            calendar.setTime(date);
        } catch (Exception e) {
            try {
                Date date = df2.parse(slice.toStringUtf8());
                calendar.setTime(date);
            } catch (ParseException ex) {
                return null;
            }
        }
        return utf8Slice(formatter.format(calendar.getTime()));
    }

    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("T")
    @SqlNullable
    public static Slice toUTCTimestampLong(
            @SqlType("T") long timestamp,
            @SqlType(StandardTypes.VARCHAR) Slice timezone) {
        if (timezone == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat1);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            calendar.setTimeInMillis(timestamp);
        } catch (Exception e) {
            return null;
        }
        return utf8Slice(formatter.format(calendar.getTime()));
    }

    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice toUTCTimestampDouble(
            @SqlType(StandardTypes.DOUBLE) double timestamp,
            @SqlType(StandardTypes.VARCHAR) Slice timezone) {
        if (timezone == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat1);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            calendar.setTimeInMillis((long)timestamp*1000);
        } catch (Exception e) {
            return null;
        }
        return utf8Slice(formatter.format(calendar.getTime()));
    }
}
