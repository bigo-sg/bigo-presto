package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import java.util.Objects;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestEncode {

    @Test
    private void testEncode() {
        String str1 = Objects.requireNonNull(EncodeFunction.encode(utf8Slice("abc"), utf8Slice("US-ASCII"))).toStringUtf8();
        String str2 = Objects.requireNonNull(EncodeFunction.encode(utf8Slice("abc"), utf8Slice("ISO-8859-1"))).toStringUtf8();
        String str3 = Objects.requireNonNull(EncodeFunction.encode(utf8Slice("abc"), utf8Slice("UTF-8"))).toStringUtf8();

        assertEquals(str1, "abc");
        assertEquals(str2, "abc");
        assertEquals(str3, "abc");
    }
}
