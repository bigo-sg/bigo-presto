package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import java.util.Objects;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestDecode {

    @Test
    private void testDecode() {
        String res1 = Objects.requireNonNull(DecodeFunction.decode(utf8Slice("abc"), utf8Slice("US-ASCII"))).toStringUtf8();
        String res2 = Objects.requireNonNull(DecodeFunction.decode(utf8Slice("abc"), utf8Slice("ISO-8859-1"))).toStringUtf8();
        String res3 = Objects.requireNonNull(DecodeFunction.decode(utf8Slice("abc"), utf8Slice("UTF-8"))).toStringUtf8();
        assertEquals(res1, "abc");
        assertEquals(res2, "abc");
        assertEquals(res3, "abc");
    }
}
