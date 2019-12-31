package io.prestosql.plugin.bigo.udf;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;

public class TestMiscFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testHash()
    {
        assertEquals(MiscFunctions.hash(utf8Slice("123")), 1067968645);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456")), -232743362);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456"), utf8Slice("789")), 723554117);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456"), utf8Slice("789"), utf8Slice("abc")), -429366540);
        assertEquals(MiscFunctions.hash(utf8Slice("123"), utf8Slice("456"), utf8Slice("789"), utf8Slice("abc"), utf8Slice("def")), -1040723660);
    }
}
