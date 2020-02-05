package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

public final class ImoUidSampleHashCode {

    @Description("generate the hash code for sampling IMO data.")
    @ScalarFunction("imoUidSampleHashCode")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long imoUidSampleHashCode(@SqlType("varchar(x)") Slice uid, @SqlType("varchar(y)") Slice key){
        int hash = 0;
        String uidkey = uid.toStringUtf8() + key.toStringUtf8();
        int h = hash;
        final int len = uidkey.length();
        if (h == 0 && len > 0) {
            for (int i = 0; i < len; i++) {
                h = 31 * h + uidkey.charAt(i);
            }
            hash = h;
        }
        return Math.abs(h);
    }

}
