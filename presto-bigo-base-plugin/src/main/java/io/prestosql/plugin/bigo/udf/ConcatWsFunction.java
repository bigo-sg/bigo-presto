package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;

public class ConcatWsFunction {
    public ConcatWsFunction() {
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    @Description("concat with a separator")
    @ScalarFunction("concat_ws")
    public static Slice concat_ws(
            @TypeParameter("T") Type elementType,
            @SqlType("T") Slice separator,
            @SqlType("array(T)") Block arrayBlock) {
        if (arrayBlock.getPositionCount() == 0) {
            return null;
        }
        String sep = separator.toStringUtf8();
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                result.append("null");
            } else {
                result.append(elementType.getSlice(arrayBlock, i).toStringUtf8());
            }
            if (i != arrayBlock.getPositionCount() - 1) {
                result.append(sep);
            }
        }

        return utf8Slice(result.toString());
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    @ScalarFunction("concat_ws")
    @Description("concat with a separator")
    public static Slice concat_ws(
            @SqlType(StandardTypes.VARCHAR) Slice separator,
            @SqlType("T") Slice value1) {
        if (separator == null) {
            return null;
        }
        return value1;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    @ScalarFunction("concat_ws")
    @Description("concat with a separator")
    public static Slice concat_ws(
            @SqlType(StandardTypes.VARCHAR) Slice separator,
            @SqlType("T") Slice value1,
            @SqlType("T") Slice value2) {
        if (separator == null) {
            return null;
        }
        if (value2 == null) {
            return value1;
        }

        String sep = separator.toStringUtf8();
        StringBuilder result = new StringBuilder();
        if (value1 != null) {
            result.append(value1.toStringUtf8());
            result.append(sep);
        }
        if (value2 != null) {
            result.append(value2.toStringUtf8());
        }
        return utf8Slice(result.toString());
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    @ScalarFunction("concat_ws")
    @Description("concat with a separator")
    public static Slice concat_ws(
            @SqlType(StandardTypes.VARCHAR) Slice separator,
            @SqlType("T") Slice value1,
            @SqlType("T") Slice value2,
            @SqlType("T") Slice value3) {
        if (separator == null) {
            return null;
        }
        if (value2 == null && value3 == null) {
            return value1;
        }

        String sep = separator.toStringUtf8();
        StringBuilder result = new StringBuilder();
        if (value1 != null) {
            result.append(value1.toStringUtf8());
            result.append(sep);
        }
        if (value2 != null) {
            result.append(value2.toStringUtf8());
            result.append(sep);
        }
        if (value3 != null) {
            result.append(value3.toStringUtf8());
        }
        return utf8Slice(result.toString());
    }
}
