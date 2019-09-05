package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestShiftRightFunction {

    @Test
    private void testShiftRightFunction()
    {
        Long s1 = ShiftRightFunction.shiftRightInt(10, 2);
        assertEquals(s1.intValue(), 2);
        Long s2 = ShiftRightFunction.shiftRightLong(10, 2);
        assertEquals(s2.intValue(), 2);
        Long s3 = ShiftRightFunction.shiftRightInt(-10, 2);
        assertEquals(s3.intValue(), -3);
    }
}
