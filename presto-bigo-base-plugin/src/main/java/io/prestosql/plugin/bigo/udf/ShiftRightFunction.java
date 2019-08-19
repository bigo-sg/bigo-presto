package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public class ShiftRightFunction {

    @Description("Shifts a b positions to the right: shiftright(a,b)")
    @ScalarFunction("shiftright")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftRightInt(@SqlType(StandardTypes.INTEGER) long left,
                                     @SqlType(StandardTypes.INTEGER) long right)
    {
        return (long)((int)left >> (int)right);
    }

    @Description("Shifts a b positions to the right: shiftright(a,b)")
    @ScalarFunction("shiftright")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftRightSmallInt(@SqlType(StandardTypes.SMALLINT) long left,
                                          @SqlType(StandardTypes.INTEGER) long right)
    {
        return (long)((int)left >> (int)right);
    }

    @Description("Shifts a b positions to the right: shiftright(a,b)")
    @ScalarFunction("shiftright")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftRightTinyInt(@SqlType(StandardTypes.TINYINT) long left,
                                          @SqlType(StandardTypes.INTEGER) long right)
    {
        return (long)((int)left >> (int)right);
    }

    @Description("Shifts a b positions to the right: shiftright(a,b)")
    @ScalarFunction("shiftright")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftRightLong(@SqlType(StandardTypes.BIGINT) long left,
                                      @SqlType(StandardTypes.INTEGER) long right)
    {
        return (left >> right);
    }

}
