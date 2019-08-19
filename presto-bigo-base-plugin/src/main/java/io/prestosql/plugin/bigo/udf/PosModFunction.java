package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public class PosModFunction {

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long pmodInt(@SqlType(StandardTypes.BIGINT) long left,
                               @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right == 0) {
            return null;
        }
        return (left % right + right) % right;
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long pmodLong(@SqlType(StandardTypes.INTEGER) long left,
                                @SqlType(StandardTypes.INTEGER) long right)
    {
        if (right == 0) {
            return null;
        }
        return (left % right + right) % right;
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodDouble(@SqlType(StandardTypes.DOUBLE) double left,
                                    @SqlType(StandardTypes.DOUBLE) double right)
    {
        if (right == 0) {
            return null;
        }
        return (left % right + right) % right;
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long pmodDecimal(@SqlType(StandardTypes.DECIMAL) long left,
                                   @SqlType(StandardTypes.DECIMAL) long right)
    {
        if (right == 0) {
            return null;
        }
        return (left % right + right) % right;
    }
    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodString(@SqlType(StandardTypes.VARCHAR) Slice leftSlice,
                                   @SqlType(StandardTypes.VARCHAR) Slice rightSlice)
    {
        try {
            double left = Double.parseDouble(leftSlice.toStringUtf8());
            double right = Double.parseDouble(rightSlice.toStringUtf8());
            if (right == 0) {
                return null;
            }
            return (left % right + right) % right;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodStringInt(@SqlType(StandardTypes.VARCHAR) Slice leftSlice,
                                       @SqlType(StandardTypes.BIGINT) long right)
    {
        try {
            double left = Double.parseDouble(leftSlice.toStringUtf8());
            if (right == 0) {
                return null;
            }
            return (left % right + right) % right;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodIntString(@SqlType(StandardTypes.BIGINT) long left,
                                       @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice rightSlice)
    {
        try {
            double right = Double.parseDouble(rightSlice.toStringUtf8());
            if (right == 0) {
                return null;
            }
            return (left % right + right) % right;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodStringDouble(@SqlType(StandardTypes.VARCHAR) Slice leftSlice, @SqlType(StandardTypes.DOUBLE) double right)
    {
        try {
            double left = Double.parseDouble(leftSlice.toStringUtf8());
            if (right == 0) {
                return null;
            }
            return (left % right + right) % right;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodDoubleString(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.VARCHAR) Slice rightSlice)
    {
        try {
            double right = Double.parseDouble(rightSlice.toStringUtf8());
            if (right == 0) {
                return null;
            }
            return (left % right + right) % right;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodStringDecimal(@SqlType(StandardTypes.VARCHAR) Slice leftSlice,
                                           @SqlType(StandardTypes.DECIMAL) long right)
    {
        try {
            double left = Double.parseDouble(leftSlice.toStringUtf8());
            if (right == 0) {
                return null;
            }
            return (left % right + right) % right;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double pmodDecimalString(@SqlType(StandardTypes.DECIMAL) long left,
                                         @SqlType(StandardTypes.VARCHAR) Slice rightSlice)
    {
        try {
            double right = Double.parseDouble(rightSlice.toStringUtf8());
            if (right == 0) {
                return null;
            }
            return (left % right + right) % right;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
