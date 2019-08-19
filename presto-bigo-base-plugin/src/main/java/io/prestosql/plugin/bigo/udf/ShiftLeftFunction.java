package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public class ShiftLeftFunction {

    @Description("Shifts a b positions to the left: shiftleft(a,b)")
    @ScalarFunction("shiftleft")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftLeftInt(@SqlType(StandardTypes.INTEGER) long left,
                                     @SqlType(StandardTypes.INTEGER) long right)
    {
        return (long)((int)left << (int)right);
    }

    @Description("Shifts a b positions to the left: shiftleft(a,b)")
    @ScalarFunction("shiftleft")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftLeftSmallInt(@SqlType(StandardTypes.SMALLINT) long left,
                                          @SqlType(StandardTypes.INTEGER) long right)
    {
        return (long)((int)left << (int)right);
    }

    @Description("Shifts a b positions to the left: shiftleft(a,b)")
    @ScalarFunction("shiftleft")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftLeftTinyInt(@SqlType(StandardTypes.TINYINT) long left,
                                         @SqlType(StandardTypes.INTEGER) long right)
    {
        return (long)((int)left << (int)right);
    }

    @Description("Shifts a b positions to the left: shiftleft(a,b)")
    @ScalarFunction("shiftleft")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long shiftLeftLong(@SqlType(StandardTypes.BIGINT) long left,
                                      @SqlType(StandardTypes.INTEGER) long right)
    {
        return (left << right);
    }

 }
