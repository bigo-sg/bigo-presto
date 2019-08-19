package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.OperatorType.EQUAL;

@Description("Determines whether given value exists in the array")
@ScalarFunction("array_contains")
public final class ArrayContainsFunction
{
    private ArrayContainsFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") Block value) throws Exception {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact((Block) elementType.getObject(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw new Exception(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") Slice value) throws Exception {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getSlice(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw new Exception(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") long value) throws Exception {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getLong(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw new Exception(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") boolean value) throws Exception {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getBoolean(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw new Exception(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") double value) throws Exception {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getDouble(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw new Exception(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    private static void checkNotIndeterminate(Boolean equalsResult)
    {
        if (equalsResult == null) {
            throw new PrestoException(NOT_SUPPORTED, "contains does not support arrays with elements that are null or contain null");
        }
    }
}
