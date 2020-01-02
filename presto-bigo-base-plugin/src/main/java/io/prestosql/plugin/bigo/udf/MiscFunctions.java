package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Murmur3Hash128;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.Objects;

public final class MiscFunctions {

    private MiscFunctions() {}

    @Description("hash function with one argument")
    @ScalarFunction("hash")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long hash(@SqlType("varchar(x)") Slice value)
    {
        // As there is a well-defined hashCode() method in Slice, we can use it directly.
        long hashValue = value.hashCode();
        return hashValue;
    }

    @Description("hash function with two argument")
    @ScalarFunction("hash")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.INTEGER)
    public static long hash(@SqlType("varchar(x)") Slice value1, @SqlType("varchar(y)") Slice value2)
    {
        long hashValue = Objects.hash(value1.hashCode(), value2.hashCode());
        return hashValue;
    }

    @Description("hash function with three argument")
    @ScalarFunction("hash")
    @LiteralParameters({"x", "y", "z"})
    @SqlType(StandardTypes.INTEGER)
    public static long hash(@SqlType("varchar(x)") Slice value1, @SqlType("varchar(y)") Slice value2, @SqlType("varchar(z)") Slice value3)
    {
        long hashValue = Objects.hash(value1.hashCode(), value2.hashCode(), value3.hashCode());
        return hashValue;
    }

    @Description("hash function with four argument")
    @ScalarFunction("hash")
    @LiteralParameters({"x", "y", "z", "u"})
    @SqlType(StandardTypes.INTEGER)
    public static long hash(@SqlType("varchar(x)") Slice value1, @SqlType("varchar(y)") Slice value2, @SqlType("varchar(z)") Slice value3, @SqlType("varchar(u)") Slice value4)
    {
        long hashValue = Objects.hash(value1.hashCode(), value2.hashCode(), value3.hashCode(), value4.hashCode());
        return hashValue;
    }

    @Description("hash function with five argument")
    @ScalarFunction("hash")
    @LiteralParameters({"x", "y", "z", "u", "t"})
    @SqlType(StandardTypes.INTEGER)
    public static long hash(@SqlType("varchar(x)") Slice value1, @SqlType("varchar(y)") Slice value2, @SqlType("varchar(z)") Slice value3, @SqlType("varchar(u)") Slice value4, @SqlType("varchar(t)") Slice value5)
    {
        long hashValue = Objects.hash(value1.hashCode(), value2.hashCode(), value3.hashCode(), value4.hashCode(), value5.hashCode());
        return hashValue;
    }

    @Description("MurmurHash3_32 function")
    @ScalarFunction("hash32")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long hash32(@SqlType("varchar(x)") Slice value)
    {
        long hashValue = Murmur3Hash32.hash(value);
        return hashValue;
    }

    @Description("MurmurHash3_128 function")
    @ScalarFunction("hash64")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long hash64(@SqlType("varchar(x)") Slice value)
    {
        long hashValue = Murmur3Hash128.hash64(value);
        return hashValue;
    }

}
