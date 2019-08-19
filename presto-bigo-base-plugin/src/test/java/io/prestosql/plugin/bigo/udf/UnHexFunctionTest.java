package io.prestosql.plugin.bigo.udf;

import io.airlift.log.Logger;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

public class UnHexFunctionTest {
    private static final Logger log = Logger.get(UnHexFunctionTest.class);

    @Test
    private void testUnHex()
    {
        String s1 = UnHexFunction.evaluateUnhex("616263");
        String s2 = UnHexFunction.evaluateUnhex("737472696E67");
        assertEquals(s1, "abc");
        assertEquals(s2, "string");
    }
}
