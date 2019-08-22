package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ShiftRightUnsignedFunctionTest {

    @Test
    private void testShiftRightUnsignedFunction()
    {
        Long s1 = ShiftRightUnsignedFunction.shiftRightInt(10, 2);
        assertEquals(s1.intValue(), 2);
        Long s2 = ShiftRightUnsignedFunction.shiftRightLongInt(10, 2);
        assertEquals(s2.intValue(), 2);
        Long s3 = ShiftRightUnsignedFunction.shiftRightInt(-10, 2);
        assertEquals(s3.longValue(), 1073741821);

    }
}
