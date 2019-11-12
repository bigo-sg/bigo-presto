package io.prestosql.plugin.bigo.udf;

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

public class BigoIsAVFeature {
    @Description("self build hive udf, isavfeature")
    @ScalarFunction("isavfeature")
    @SqlType(StandardTypes.BOOLEAN)
    @TypeParameter("T")
    public static boolean isAvFeature(@SqlType("T") long k,
                                      @SqlType("T") long featureMask) {
        if (k > 64) {
            return false;
        }
        return (featureMask & (1 << k)) != 0;
    }
}
