package io.prestosql.plugin.bigo.udf;

import io.airlift.log.Logger;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestPosModFunction {
    private static final Logger log = Logger.get(TestPosModFunction.class);

    @Test
    private void testPosModFunction()
    {
        log.info("test_PosModFunctionTest.");
        Long s1 = PosModFunction.pmodInt(10, 3);
        Long s2 = PosModFunction.pmodInt(10, -3);
        Long s3 = PosModFunction.pmodInt(-10, 3);
        Long s4 = PosModFunction.pmodInt(-10, -3);

        Double s5 = PosModFunction.pmodDoubleString(10, utf8Slice("3"));
        Double s6 = PosModFunction.pmodStringDouble(utf8Slice("10"), 3);
        Double s7 = PosModFunction.pmodString(utf8Slice("10"), utf8Slice("3"));

        assert s1 != null;
        assertEquals(s1.intValue(), 1);
        assert s2 != null;
        assertEquals(s2.intValue(), -2);
        assert s3 != null;
        assertEquals(s3.intValue(), 2);
        assert s4 != null;
        assertEquals(s4.intValue(), -1);
        assert s5 != null;
        assertEquals(s5, 1.0);
        assert s6 != null;
        assertEquals(s6, 1.0);
        assert s7 != null;
        assertEquals(s7, 1.0);

    }
}
