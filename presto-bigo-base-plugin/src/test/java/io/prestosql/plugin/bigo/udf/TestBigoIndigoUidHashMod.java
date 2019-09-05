package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestBigoIndigoUidHashMod {

    @Test
    private void testBigoIndigoUidHash() {
        long res1 = BigoIndigoUidHashMod.evaluate("test", 123456L);
        assertEquals(res1, 58);
    }
}
