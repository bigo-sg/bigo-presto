package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

public class HexFunctionTest {

    @Test
    private void testHex()
    {
        String s1 = HexFunction.evaluate(12345);
        String s2 = HexFunction.evaluate("abc");
        String s3 = HexFunction.evaluate("string");
        assertEquals(s1, "3039");
        assertEquals(s2, "616263");
        assertEquals(s3, "737472696E67");
    }
}
