package io.prestosql.plugin.bigo.udf;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

@Description("Returns the number of elements in the array.")
@ScalarFunction("size")
public final class ArraySizeFunction
{
    private ArraySizeFunction() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BIGINT)
    public static long arraySize(@SqlType("array(E)") Block block)
    {
        return block.getPositionCount();
    }
}
