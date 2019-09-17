package io.prestosql.plugin.bigo.udf;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestShaFunction
        extends AbstractTestFunctions
{
    @Test
    public void testSHA()
    {
        assertEquals(ShaFunction.sha(utf8Slice("ABC")).toStringUtf8(), "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8");
    }
}
