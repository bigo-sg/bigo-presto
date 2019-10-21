package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

//import org.apache.commons.codec.binary.Base64;;

import java.util.Base64;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class BigoVarbinaryFunctions {
    @Description("decode base64 encoded binary data")
    @ScalarFunction("unbase64")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    @SqlNullable
    public static Slice unbase64Varchar(@SqlType("varchar(x)") Slice slice)
    {
        try {
//            return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.toStringUtf8().replace("\r\n", "")));
            return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.toStringUtf8().replace("\r\n", "")));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
//            return null;
        }
    }

    @Description("encode binary data as base64 using the URL safe alphabet")
    @ScalarFunction("base64")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toBase64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return Slices.wrappedBuffer(Base64.getEncoder().encode(slice.getBytes()));
    }
}
