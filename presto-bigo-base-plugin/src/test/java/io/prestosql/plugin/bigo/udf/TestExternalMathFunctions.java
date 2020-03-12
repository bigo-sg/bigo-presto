package io.prestosql.plugin.bigo.udf;

import io.prestosql.plugin.bigo.FunctionsTestUtil;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

public class TestExternalMathFunctions extends FunctionsTestUtil
{
    @Test
    public void testConv() {
        assertFunction("conv('2e', 16, 10)", VarcharType.VARCHAR, "46");
    }

    @Test
    public void testBin() {
        assertFunction("bin(12)", VarcharType.VARCHAR, "1100");
    }

}
