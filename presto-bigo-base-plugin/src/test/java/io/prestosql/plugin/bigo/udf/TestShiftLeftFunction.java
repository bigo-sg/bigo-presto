package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestShiftLeftFunction {

    @Test
    private void testShiftRightFunction()
    {
        Long s1 = ShiftLeftFunction.shiftLeftInt(10, 2);
        assertEquals(s1.intValue(), 40);
        Long s2 = ShiftLeftFunction.shiftLeftLong(10, 2);
        assertEquals(s2.intValue(), 40);
        Long s3 = ShiftLeftFunction.shiftLeftInt(-10, 2);
        assertEquals(s3.intValue(), -40);
    }
}
