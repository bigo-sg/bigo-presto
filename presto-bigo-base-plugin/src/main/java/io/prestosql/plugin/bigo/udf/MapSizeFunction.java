package io.prestosql.plugin.bigo.udf;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

@Description("Returns the number of elements in the map.")
@ScalarFunction("size")
public final class MapSizeFunction
{
    private MapSizeFunction() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BIGINT)
    public static long mapCardinality(@SqlType("map(K,V)") Block block)
    {
        return block.getPositionCount() / 2;
    }
}
