package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public final class BinaryFunction {
    private BinaryFunction() {
    }

    @Description("Casts the parameter into a binary.")
    @ScalarFunction("binary")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice binary(@SqlType(StandardTypes.VARBINARY) Slice slice) {
        return slice;
    }

    @Description("Casts the parameter into a binary.")
    @ScalarFunction("binary")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice binary_varchar(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        return slice;
    }

    @Description("Casts the parameter into a binary.")
    @ScalarFunction("binary")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice binary_char(@SqlType(StandardTypes.CHAR) Slice slice) {
        return slice;
    }
}
