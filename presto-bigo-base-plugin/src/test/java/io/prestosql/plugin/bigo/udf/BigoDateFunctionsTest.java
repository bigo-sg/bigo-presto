package io.prestosql.plugin.bigo.udf;

import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

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
        assertEquals(uts, 1563724800);
    }

    @Test
    public void testUnixTimestampWithFormat()
    {
        double uts = unixTimestamp(utf8Slice("2019-07-22"), utf8Slice("yyyy-MM-dd"));
        assertEquals(uts, 1563724800);
    }
}