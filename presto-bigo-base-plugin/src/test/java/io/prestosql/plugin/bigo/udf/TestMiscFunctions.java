package io.prestosql.plugin.bigo.udf;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestMiscFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testHash()
    {
        assertEquals(MiscFunctions.hash(utf8Slice("123")), 1067968645L);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456")), -232743362L);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456"), utf8Slice("789")), 723554117L);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456"), utf8Slice("789"), utf8Slice("abc")), -429366540L);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456"), utf8Slice("789"), utf8Slice("abc"), utf8Slice("def")), -1040723660L);
    }

    @Test
    public void testHash32() {
        assertEquals(MiscFunctions.hash32(utf8Slice("123")), -1632341525L);
    }

    @Test
    public void testHash64() {
        assertEquals(MiscFunctions.hash64(utf8Slice("123")), -7468325962851647638L);
    }

}
