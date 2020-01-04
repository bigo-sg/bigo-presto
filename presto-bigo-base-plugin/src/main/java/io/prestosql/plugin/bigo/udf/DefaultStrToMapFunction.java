package io.prestosql.plugin.bigo.udf;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;


@Description("creates a map using entryDelimiter and keyValueDelimiter")
@ScalarFunction("str_to_map")
public class DefaultStrToMapFunction
{
    private final PageBuilder pageBuilder;

    public DefaultStrToMapFunction(@TypeParameter("map(varchar,varchar)") Type mapType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(mapType));
    }

    @SqlType("map(varchar,varchar)")
    public Block strToMap(@TypeParameter("map(varchar,varchar)") Type mapType, @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        StrToMapFunction strToMapFunction = new StrToMapFunction(mapType);
        return strToMapFunction.strToMap(mapType, string, utf8Slice(","), utf8Slice(":"));
    }
}

