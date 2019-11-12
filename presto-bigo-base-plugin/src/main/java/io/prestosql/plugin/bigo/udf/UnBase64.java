package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.Base64;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class UnBase64 {
    @Description("decode base64 encoded binary data")
    @ScalarFunction("unbase64")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice unBase64Varchar(@SqlType("varchar(x)") Slice slice) {
        try {
            return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.getBytes()));
        } catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("decode base64 encoded binary data")
    @ScalarFunction("unbase64")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice unBase64Varbinary(@SqlType(StandardTypes.VARBINARY) Slice slice) {
        try {
            return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.getBytes()));
        } catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
