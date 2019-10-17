package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestBigoStringFunctions {

    @Test
    public void testStrPosAndPosition()
    {
        long pos = BigoStringFunctions.inStr(utf8Slice("bigolive"), utf8Slice("go"));

        assertEquals(pos, 3);
    }

    @Test
    public void testSubstr()
    {
        Slice res1 = BigoStringFunctions.substring(utf8Slice("hello"), 3);
        Slice res2 = BigoStringFunctions.substring(utf8Slice("hello"), 3, 2);

        assertEquals(res1.toStringUtf8(), "llo");
        assertEquals(res2.toStringUtf8(), "ll");
    }

    @Test
    public void testStrPos()
    {
        assertEquals(BigoStringFunctions.stringPosition(utf8Slice("abcdabcd"), utf8Slice("b"), 2), 6);
        assertEquals(BigoStringFunctions.stringPosition(utf8Slice("hello"), utf8Slice("l"), 1), 3);
        assertEquals(BigoStringFunctions.stringPosition(utf8Slice("hello"), utf8Slice("world"), 1), 0);
    }
}