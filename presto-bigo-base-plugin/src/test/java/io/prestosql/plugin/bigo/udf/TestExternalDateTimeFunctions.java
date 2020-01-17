package io.prestosql.plugin.bigo.udf;

import io.prestosql.plugin.bigo.FunctionsTestUtil;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

public class TestExternalDateTimeFunctions extends FunctionsTestUtil
{
    @Test
    public void testFromUnixTime() {
        assertFunction("from_unixtime(1578175938)", VarcharType.VARCHAR, "2020-01-04 16:12:18");
    }

}
