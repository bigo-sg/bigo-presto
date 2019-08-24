package io.prestosql.plugin.bigo.udf;

import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.bigo.udf.BigoDateFunctions.unixTimestamp;
import static org.testng.Assert.*;

public class BigoDateFunctionsTest {

    @Test
    public void testDateAdd() {
        String output = BigoDateFunctions.dateAdd(utf8Slice("2019-01-01"), 10).toStringUtf8();

        assertEquals(output, "2019-01-11");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void dateAddWithInvalidDateStringShouldThrowException() {
        BigoDateFunctions.dateAdd(utf8Slice("2019-aaaa-01"), 10).toStringUtf8();
    }

    @Test
    public void testDateSub() {
        String output = BigoDateFunctions.dateSub(utf8Slice("2019-01-11"), 10).toStringUtf8();

        assertEquals(output, "2019-01-01");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void dateSubWithInvalidDateStringShouldThrowException() {
        BigoDateFunctions.dateSub(utf8Slice("2019-aaaa-01"), 10).toStringUtf8();
    }


    @Test
    public void testUnixTimestamp()
    {
        double uts = unixTimestamp(utf8Slice("2019-07-22 00:00:00"));
        if(String.valueOf(uts).contains("E")){
            BigDecimal bd1 = new BigDecimal(uts);
            assertEquals(bd1.toPlainString(), "1563724800");
        }else {
            assertEquals(String.valueOf(uts), "1563724800");
        }
    }

    @Test
    public void testUnixTimestampWithFormat()
    {
        double uts = unixTimestamp(utf8Slice("2019-07-22"), utf8Slice("yyyy-MM-dd"));
        if(String.valueOf(uts).contains("E")){
            BigDecimal bd1 = new BigDecimal(uts);
            assertEquals(bd1.toPlainString(), "1563724800");
        }else {
            assertEquals(String.valueOf(uts), "1563724800");
        }
    }

    @Test
    public void testDateDiff()
    {
        long diff = BigoDateFunctions.dateDiff(utf8Slice("2019-01-03"), utf8Slice("2018-12-31"));
        assertEquals(diff, 3);
    }

    @Test
    public void testHourFromString()
    {
        long hour1 = BigoDateFunctions.hourFromString(utf8Slice("2019-08-01 12:13:14"));
        long hour2 = BigoDateFunctions.hourFromString(utf8Slice("2019-08-01 24:13:14"));
        long hour3 = BigoDateFunctions.hourFromString(utf8Slice("12:13:14"));
        long hour4 = BigoDateFunctions.hourFromString(utf8Slice("24:13:14"));
        assertEquals(hour1, 12);
        assertEquals(hour2, 0);
        assertEquals(hour3, 12);
        assertEquals(hour4, 0);
    }

    @Test
    public void testMinuteFromString()
    {
        long minute1 = BigoDateFunctions.minuteFromString(utf8Slice("2019-08-01 12:13:14"));
        long minute2 = BigoDateFunctions.minuteFromString(utf8Slice("12:13:14"));
        assertEquals(minute1, 13);
        assertEquals(minute2, 13);
    }

    @Test
    public void testSecondFromString()
    {
        long second1 = BigoDateFunctions.secondFromString(utf8Slice("2019-08-01 12:13:14"));
        long second2 = BigoDateFunctions.secondFromString(utf8Slice("12:13:14"));
        assertEquals(second1, 14);
        assertEquals(second2, 14);
    }

    @Test
    public void testDayFromString()
    {
        long day1 = BigoDateFunctions.dayFromString(utf8Slice("2019-08-01 12:13:14"));
        long day2 = BigoDateFunctions.dayFromString(utf8Slice("2019-08-01"));
        assertEquals(day1, 1);
        assertEquals(day2, 1);
    }

    @Test
    public void testMonthFromString()
    {
        long month1 = BigoDateFunctions.monthFromString(utf8Slice("2019-08-01 12:13:14"));
        long month2 = BigoDateFunctions.monthFromString(utf8Slice("2019-08-01"));
        assertEquals(month1, 8);
        assertEquals(month2, 8);
    }

    @Test
    public void testYearFromString()
    {
        long year1 = BigoDateFunctions.yearFromString(utf8Slice("2019-08-01 12:13:14"));
        long year2 = BigoDateFunctions.yearFromString(utf8Slice("2019-08-01"));
        assertEquals(year1, 2019);
        assertEquals(year2, 2019);
    }

    @Test
    public void testQuarterFromString()
    {
        long quarter1 = BigoDateFunctions.quarterFromString(utf8Slice("2019-08-01 12:13:14"));
        long quarter2 = BigoDateFunctions.quarterFromString(utf8Slice("2019-06-01"));
        assertEquals(quarter1, 3);
        assertEquals(quarter2, 2);
    }
}