package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestAsciiFunction {

    @Test
    private void testAscii()
    {
        long r1 = AsciiFunction.ascii(utf8Slice("a"));
        long r2 = AsciiFunction.ascii(utf8Slice("abc"));
        long r3 = AsciiFunction.ascii(utf8Slice("123"));

        assertEquals(r1, 97);
        assertEquals(r2, 97);
        assertEquals(r3, 49);
    }
}
