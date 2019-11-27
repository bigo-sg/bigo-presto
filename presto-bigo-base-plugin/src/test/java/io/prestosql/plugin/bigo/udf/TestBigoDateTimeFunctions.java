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
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.testing.TestingSession;
import org.locationtech.jts.util.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.bigo.udf.BigoDateTimeFunctions.unixTimestamp;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestBigoDateTimeFunctions {
    @Test
    public void testDateAdd() {
        String output = BigoDateTimeFunctions.dateAdd(utf8Slice("2019-01-01"), 10).toStringUtf8();

        assertEquals(output, "2019-01-11");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void dateAddWithInvalidDateStringShouldThrowException() {
        BigoDateTimeFunctions.dateAdd(utf8Slice("2019-aaaa-01"), 10).toStringUtf8();
    }

    @Test
    public void testDateSub() {
        String output = BigoDateTimeFunctions.dateSub(utf8Slice("2019-01-11"), 10).toStringUtf8();

        assertEquals(output, "2019-01-01");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void dateSubWithInvalidDateStringShouldThrowException() {
        BigoDateTimeFunctions.dateSub(utf8Slice("2019-aaaa-01"), 10).toStringUtf8();
    }


    @Test
    public void testUnixTimestamp() {
        double uts = unixTimestamp(utf8Slice("2019-08-31 12:00:00"));
        if (String.valueOf(uts).contains("E")) {
            BigDecimal bd1 = new BigDecimal(uts);
            assertEquals(bd1.toPlainString(), "1567224000");
        } else {
            assertEquals(String.valueOf(uts), "1567224000");
        }
    }

    @Test
    public void testUnixTimestampWithFormat() {
        double uts = unixTimestamp(utf8Slice("2019-07-22"), utf8Slice("yyyy-MM-dd"));
        if (String.valueOf(uts).contains("E")) {
            BigDecimal bd1 = new BigDecimal(uts);
            assertEquals(bd1.toPlainString(), "1563724800");
        } else {
            assertEquals(String.valueOf(uts), "1563724800");
        }
    }

    @Test
    public void testDateDiff() {
        long diff = BigoDateTimeFunctions.dateDiff(utf8Slice("2019-01-03"), utf8Slice("2018-12-31"));
        assertEquals(diff, 3);
    }

    @Test
    public void testHourFromString() {
        long hour1 = BigoDateTimeFunctions.hourFromString(utf8Slice("2019-08-01 12:13:14"));
        long hour2 = BigoDateTimeFunctions.hourFromString(utf8Slice("2019-08-01 24:13:14"));
        long hour3 = BigoDateTimeFunctions.hourFromString(utf8Slice("12:13:14"));
        long hour4 = BigoDateTimeFunctions.hourFromString(utf8Slice("24:13:14"));
        assertEquals(hour1, 12);
        assertEquals(hour2, 0);
        assertEquals(hour3, 12);
        assertEquals(hour4, 0);
    }

    @Test
    public void testMinuteFromString() {
        long minute1 = BigoDateTimeFunctions.minuteFromString(utf8Slice("2019-08-01 12:13:14"));
        long minute2 = BigoDateTimeFunctions.minuteFromString(utf8Slice("12:13:14"));
        assertEquals(minute1, 13);
        assertEquals(minute2, 13);
    }

    @Test
    public void testSecondFromString() {
        long second1 = BigoDateTimeFunctions.secondFromString(utf8Slice("2019-08-01 12:13:14"));
        long second2 = BigoDateTimeFunctions.secondFromString(utf8Slice("12:13:14"));
        assertEquals(second1, 14);
        assertEquals(second2, 14);
    }

    @Test
    public void testDayFromString() {
        long day1 = BigoDateTimeFunctions.dayFromString(utf8Slice("2019-08-01 12:13:14"));
        long day2 = BigoDateTimeFunctions.dayFromString(utf8Slice("2019-08-01"));
        assertEquals(day1, 1);
        assertEquals(day2, 1);
    }

    @Test
    public void testMonthFromString() {
        long month1 = BigoDateTimeFunctions.monthFromString(utf8Slice("2019-08-01 12:13:14"));
        long month2 = BigoDateTimeFunctions.monthFromString(utf8Slice("2019-08-01"));
        assertEquals(month1, 8);
        assertEquals(month2, 8);
    }

    @Test
    public void testYearFromString() {
        long year1 = BigoDateTimeFunctions.yearFromString(utf8Slice("2019-08-01 12:13:14"));
        long year2 = BigoDateTimeFunctions.yearFromString(utf8Slice("2019-08-01"));
        assertEquals(year1, 2019);
        assertEquals(year2, 2019);
    }

    @Test
    public void testQuarterFromString() {
        long quarter1 = BigoDateTimeFunctions.quarterFromString(utf8Slice("2019-08-01 12:13:14"));
        long quarter2 = BigoDateTimeFunctions.quarterFromString(utf8Slice("2019-06-01"));
        assertEquals(quarter1, 3);
        assertEquals(quarter2, 2);
    }

    @Test
    public void testCalPt() {
        assertEquals(BigoDateTimeFunctions.cal_pt(utf8Slice("PT10.2S")), 10.2);
        assertEquals(BigoDateTimeFunctions.cal_pt(utf8Slice("PT1.0M10.2S")), 70.2);
        assertEquals(BigoDateTimeFunctions.cal_pt(utf8Slice("PT1.0H1M10.2S")), 3670.2);
    }

    @Test
    public void testAddMonths() {
        assertEquals(BigoDateTimeFunctions.addMonths(utf8Slice("2019-08-01"), 2).toStringUtf8(), "2019-10-01");
        assertEquals(BigoDateTimeFunctions.addMonths(utf8Slice("2019-08-31"), 1).toStringUtf8(), "2019-09-30");
        assertEquals(BigoDateTimeFunctions.addMonths(utf8Slice("2018-08-31"), 13).toStringUtf8(), "2019-09-30");
        assertEquals(BigoDateTimeFunctions.addMonths(utf8Slice("2008-08-31"), 133).toStringUtf8(), "2019-09-30");

        assertEquals(BigoDateTimeFunctions.addMonths(utf8Slice("2019-08-01"), 2, utf8Slice("yyyy-MM-dd")).toStringUtf8(), "2019-10-01");
        assertEquals(BigoDateTimeFunctions.addMonths(utf8Slice("2019-08-31 14:15:16"), 1, utf8Slice("yyyy-MM-dd HH:mm:ss")).toStringUtf8(), "2019-09-30 14:15:16");
        assertEquals(BigoDateTimeFunctions.addMonthsDate(18139, 1).toStringUtf8(), "2019-09-30");
    }

    @Test
    public void testTruncateTime()
    {
        ConnectorSession connectorSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test"))
                .setCatalog("hive")
                .setSchema("default")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))
                .build()
                .toConnectorSession();

        long res1 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("second"));
        long res2 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("minute"));
        long res3 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("hour"));
        long res4 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("day"));
        long res5 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("week"));
        long res6 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("month"));
        long res7 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("quarter"));
        long res8 = BigoDateTimeFunctions.truncateTimestamp(connectorSession, 1574050394000L, utf8Slice("year"));

        Assert.equals(res1, 1574050394000L);
        Assert.equals(res2, 1574050380000L);
        Assert.equals(res3, 1574049600000L);
        Assert.equals(res4, 1574006400000L);
        Assert.equals(res5, 1574006400000L);
        Assert.equals(res6, 1572537600000L);
        Assert.equals(res7, 1569859200000L);
        Assert.equals(res8, 1546272000000L);
    }

    @Test
    public void testTruncateVarchar()
    {
        ConnectorSession connectorSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test"))
                .setCatalog("hive")
                .setSchema("default")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))
                .build()
                .toConnectorSession();

        Slice res1 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18 12:13:14"), utf8Slice("month"));
        Slice res2 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18 12:13:14"), utf8Slice("year"));
        Slice res3 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18"), utf8Slice("month"));
        Slice res4 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18"), utf8Slice("mon"));
        Slice res5 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18"), utf8Slice("mm"));
        Slice res6 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18"), utf8Slice("year"));
        Slice res7 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18"), utf8Slice("yyyy"));
        Slice res8 = BigoDateTimeFunctions.truncateVarchar(connectorSession, utf8Slice("2019-11-18"), utf8Slice("yy"));

        Assert.equals(res1, utf8Slice("2019-11-01"));
        Assert.equals(res2, utf8Slice("2019-01-01"));
        Assert.equals(res3, utf8Slice("2019-11-01"));
        Assert.equals(res4, utf8Slice("2019-11-01"));
        Assert.equals(res5, utf8Slice("2019-11-01"));
        Assert.equals(res6, utf8Slice("2019-01-01"));
        Assert.equals(res7, utf8Slice("2019-01-01"));
        Assert.equals(res8, utf8Slice("2019-01-01"));
    }

    @Test
    public void testDateFormat()
    {
        assertNull(BigoDateTimeFunctions.dateFormat(null,null));
        assertNull(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"),null));
        assertNull(BigoDateTimeFunctions.dateFormat(null, utf8Slice("test")));

        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("Y")).toStringUtf8(), "2019");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("y")).toStringUtf8(), "2019");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("M")).toStringUtf8(), "11");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("d")).toStringUtf8(), "22");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("H")).toStringUtf8(), "12");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("m")).toStringUtf8(), "13");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("s")).toStringUtf8(), "14");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("YYYY")).toStringUtf8(), "2019");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("YYYYMM")).toStringUtf8(), "201911");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("YYYYMMdd")).toStringUtf8(), "20191122");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22 12:13:14"), utf8Slice("YYYY-MM-dd")).toStringUtf8(), "2019-11-22");

        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22"), utf8Slice("Y")).toStringUtf8(), "2019");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22"), utf8Slice("y")).toStringUtf8(), "2019");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22"), utf8Slice("M")).toStringUtf8(), "11");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22"), utf8Slice("d")).toStringUtf8(), "22");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22"), utf8Slice("YYYY")).toStringUtf8(), "2019");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22"), utf8Slice("YYYYMM")).toStringUtf8(), "201911");
        assertEquals(BigoDateTimeFunctions.dateFormat(utf8Slice("2019-11-22"), utf8Slice("YYYYMMdd")).toStringUtf8(), "20191122");
    }

    @Test
    public void testToUTCTimestamp()
    {
        assertNull(BigoDateTimeFunctions.toUTCTimestamp(null,null));
        assertNull(BigoDateTimeFunctions.toUTCTimestamp(utf8Slice("1970-01-30"),null));
        assertNull(BigoDateTimeFunctions.toUTCTimestamp(null, utf8Slice("PST")));

        assertEquals(BigoDateTimeFunctions.toUTCTimestamp(utf8Slice("1970-01-30 16:00:00"), utf8Slice("PST")), utf8Slice("1970-01-31 00:00:00"));
        assertEquals(BigoDateTimeFunctions.toUTCTimestamp(utf8Slice("1970-01-31 16:00:00"), utf8Slice("PST")), utf8Slice("1970-02-01 00:00:00"));

        assertEquals(BigoDateTimeFunctions.toUTCTimestamp(utf8Slice("1970-01-30"), utf8Slice("PST")), utf8Slice("1970-01-30 08:00:00"));
        assertEquals(BigoDateTimeFunctions.toUTCTimestamp(utf8Slice("1970-01-30 16:00:00"), utf8Slice("test")), utf8Slice("1970-01-30 16:00:00"));

        assertEquals(BigoDateTimeFunctions.toUTCTimestampLong(2592000000L, utf8Slice("PST")).toStringUtf8(), "1970-01-31 00:00:00");
        assertEquals(BigoDateTimeFunctions.toUTCTimestampDouble(2592000.0, utf8Slice("PST")).toStringUtf8(), "1970-01-31 00:00:00");
    }
}
