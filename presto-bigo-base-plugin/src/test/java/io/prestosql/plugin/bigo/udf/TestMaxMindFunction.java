package io.prestosql.plugin.bigo.udf;

import io.prestosql.spi.block.Block;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;

public class TestMaxMindFunction {

    @Test
    public void testIp2Country() {
        Block block = MaxMindFunction.ip2Country(utf8Slice("128.101.101.101"), utf8Slice("20200219"));
        System.out.println(block);
    }

}
