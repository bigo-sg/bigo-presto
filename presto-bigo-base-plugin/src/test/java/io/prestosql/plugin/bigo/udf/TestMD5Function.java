package io.prestosql.plugin.bigo.udf;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestMD5Function
        extends AbstractTestFunctions
{
    @Test
    public void testMd5()
    {
        assertEquals(MD5Function.md5(utf8Slice("hello")).toStringUtf8(), "5d41402abc4b2a76b9719d911017c592");
        assertEquals(MD5Function.md5(utf8Slice("hello, world.")).toStringUtf8(), "708171654200ecd0e973167d8826159c");
    }
}
