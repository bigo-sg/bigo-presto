package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestBigoConditionalFunctions {

    @Test
    private void testNvl()
    {
        Slice s1 = BigoConditionalFunctions.nvlString(null, utf8Slice("test"));
        Slice s2 = BigoConditionalFunctions.nvlString(utf8Slice("abc"), utf8Slice("test"));
        long s3 = BigoConditionalFunctions.nvlInt(null, 1);
        long s4 = BigoConditionalFunctions.nvlInt((long) 2, 1);
        double s5 = BigoConditionalFunctions.nvlDouble(null, 1.0);
        double s6 = BigoConditionalFunctions.nvlDouble(2.0, 1.0);
        assertEquals(s1, utf8Slice("test"));
        assertEquals(s2, utf8Slice("abc"));
        assertEquals(s3, 1);
        assertEquals(s4, 2);
        assertEquals(s5, 1.0);
        assertEquals(s6, 2.0);

    }
}
