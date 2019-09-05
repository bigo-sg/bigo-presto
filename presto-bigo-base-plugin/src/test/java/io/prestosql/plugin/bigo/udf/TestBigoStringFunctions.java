package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestBigoStringFunctions {

    @Test
    private void testStrPosAndPosition()
    {
        long pos = BigoStringFunctions.inStr(utf8Slice("bigolive"), utf8Slice("go"));

        assertEquals(pos, 3);
    }

    @Test
    private void testSubstr()
    {
        Slice res1 = BigoStringFunctions.substring(utf8Slice("hello"), 3);
        Slice res2 = BigoStringFunctions.substring(utf8Slice("hello"), 3, 2);

        assertEquals(res1.toStringUtf8(), "llo");
        assertEquals(res2.toStringUtf8(), "ll");
    }

}