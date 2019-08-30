package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

public class TestBigoIsAVTest {

    @Test
    private void testIsAvTest() {
        boolean res1 = BigoIsAVTest.evaluate(123, "test", 456);
        assertFalse(res1);
    }
}
