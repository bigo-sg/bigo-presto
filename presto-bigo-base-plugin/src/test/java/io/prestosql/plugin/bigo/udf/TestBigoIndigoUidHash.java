package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestBigoIndigoUidHash {

    @Test
    private void testBigoIndigoUidHash()
    {
        String str1 = BigoIndigoUidHash.evaluate("123");
        String str2 = BigoIndigoUidHash.evaluate("123456789");

        assertEquals(str1, "3222588021317909685");
        assertEquals(str2, "7102376421209267306");
    }
}
