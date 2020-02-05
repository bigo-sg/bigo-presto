package io.prestosql.plugin.bigo.udf;

import io.prestosql.plugin.bigo.FunctionsTestUtil;
import io.prestosql.spi.type.BigintType;
import org.testng.annotations.Test;

public class TestImoUidSampleHashCode extends FunctionsTestUtil
{
    @Test
    public void testFromUnixTime() {
        assertFunction("imoUidSampleHashCode('1578175938', 'test')", BigintType.BIGINT, 364649990L);
    }

}
