package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestBigoDecodeUrl {

    @Test
    private void testDecodeUrl() {
        String res1 = BigoDecodeUrl.evaluate("test");
        assertEquals(res1, "test");
    }
}
