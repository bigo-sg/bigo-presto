package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.JsonPathType;

public class BigoJsonFunctions {
    private BigoJsonFunctions() {}

    @ScalarFunction("get_json_object")
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice varcharGetJsonObject(@SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        if (JsonExtract.extract(json, jsonPath.getScalarExtractor()) == null){
            return JsonExtract.extract(json, jsonPath.getObjectExtractor());
        } else {
            return JsonExtract.extract(json, jsonPath.getScalarExtractor());
        }
    }
}
