package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.IntegerType.INTEGER;

public class TestBigoArrayFunction
        extends AbstractTestFunctions {

    @Test
    public void testArray() {
        assertFunction("array(1,2)", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("array(1,2,3,4,5,4,3,2,1)", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3, 4, 5, 4, 3, 2, 1));
        assertFunction("array('a')", new ArrayType(VarcharType.createVarcharType(1)), ImmutableList.of("a"));
        assertFunction("array('hello','hi')", new ArrayType(VarcharType.createVarcharType(5)), ImmutableList.of("hello", "hi"));
    }
}
