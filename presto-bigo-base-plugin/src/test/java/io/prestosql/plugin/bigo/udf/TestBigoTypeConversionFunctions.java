package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestBigoTypeConversionFunctions {

    @Test
    private void testInt()
    {
        long r1 = BigoTypeConversionFunctions.intFunction(1L);
        long r2 = BigoTypeConversionFunctions.intFunctionDouble(1.2);
        long r3 = BigoTypeConversionFunctions.intFunctionInt(1);
        Long r4 = BigoTypeConversionFunctions.intFunctionSlice(utf8Slice("10"));
        long r5 = BigoTypeConversionFunctions.intFunctionInt(9223372036854775807L);
        long r6 = BigoTypeConversionFunctions.intFunctionInt(-9223372036854775808L);
        long r7 = BigoTypeConversionFunctions.intFunctionInt(2147483647);
        long r8 = BigoTypeConversionFunctions.intFunctionInt(-2147483648);

        assertEquals(r1, 1);
        assertEquals(r2, 1);
        assertEquals(r3, 1);
        assert r4 != null;
        assertEquals(r4.intValue(), 10);
        assertEquals(r5, -1);
        assertEquals(r6, 0);
        assertEquals(r7, 2147483647);
        assertEquals(r8, -2147483648);
    }

    @Test
    private void testBigint(){
        assertEquals(BigoTypeConversionFunctions.bigintFunctionBigint(100L).longValue(), 100L);
        assertNull(BigoTypeConversionFunctions.bigintFunctionSlice(utf8Slice("")));
    }

    @Test
    private void testDouble()
    {
        double r1 = BigoTypeConversionFunctions.doubleFunction(12.3);
        double r2 = BigoTypeConversionFunctions.doubleFunctionInt(12L);
        Double r3 = BigoTypeConversionFunctions.doubleFunctionSlice(utf8Slice("12.3"));
        Double r4 = BigoTypeConversionFunctions.doubleFunctionSlice(utf8Slice("92233720368547758079223372036854775807"));

        double r5 = BigoTypeConversionFunctions.doubleFunctionInt(9223372036854775807L);
        double r6 = BigoTypeConversionFunctions.doubleFunctionInt(-9223372036854775808L);
        double r7 = BigoTypeConversionFunctions.doubleFunction(1e100);

        assertEquals(r1, 12.3);
        assertEquals(r2, 12.0);
        assertEquals(r3, 12.3);
        assertEquals(r4, 9.223372036854776E37);
        assertEquals(r5, 9.223372036854776E18);
        assertEquals(r6, -9.223372036854776E18);
        assertEquals(r7, 1.0E100);
    }

    @Test
    private void testString()
    {
        Slice r1 = BigoTypeConversionFunctions.stringFunctionInt(12);
        Slice r2 = BigoTypeConversionFunctions.stringFunctionDouble(12.3);
        Slice r3 = BigoTypeConversionFunctions.stringFunction(utf8Slice("12.3"));

        assertEquals(r1.toStringUtf8(), "12");
        assertEquals(r2.toStringUtf8(), "12.3");
        assertEquals(r3.toStringUtf8(), "12.3");
    }
}
