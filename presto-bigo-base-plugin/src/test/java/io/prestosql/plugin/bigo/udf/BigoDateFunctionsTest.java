package io.prestosql.plugin.bigo.udf;

import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
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

}