package io.prestosql.sql.analyzer;

import io.prestosql.spi.type.*;
import org.testng.annotations.Test;

public class TestTypeConversion {
    private static final TypeConversion tc = new TypeConversion();
    private static final Type booleanType = BooleanType.BOOLEAN;
    private static final Type tinyType = TinyintType.TINYINT;
    private static final Type smallType = SmallintType.SMALLINT;
    private static final Type intType = IntegerType.INTEGER;
    private static final Type bigIntType = BigintType.BIGINT;
    private static final Type doubleType = DoubleType.DOUBLE;
    private static final Type decimalType = DecimalType.createDecimalType();
    private static final Type varcharType = VarcharType.VARCHAR;
    private static final Type timestampType = TimestampType.TIMESTAMP;
    private static final Type dateType = DateType.DATE;

    @Test
    public void testCompare2TypesOrder() {
        assert(tc.compare2TypesOrder(null, null) == null);
        assert(tc.compare2TypesOrder(null, doubleType) == null);
        assert(tc.compare2TypesOrder(doubleType, null) == null);
        assert (tc.compare2TypesOrder(booleanType, booleanType) == null);
        assert (tc.compare2TypesOrder(booleanType, intType) == null);
        assert (tc.compare2TypesOrder(tinyType, smallType) == smallType);
        assert (tc.compare2TypesOrder(intType, doubleType) == doubleType);
        assert (tc.compare2TypesOrder(bigIntType, doubleType) == doubleType);
        assert (tc.compare2TypesOrder(doubleType, intType) == doubleType);
        assert (tc.compare2TypesOrder(decimalType, doubleType) == null);
        assert (tc.compare2TypesOrder(intType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(doubleType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(timestampType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(dateType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(timestampType, timestampType) == null);
        assert (tc.compare2TypesOrder(dateType, dateType) == null);
        assert (tc.compare2TypesOrder(dateType, doubleType) == null);
        assert (tc.compare2TypesOrder(varcharType, intType) == null);
    }

    @Test
    public void testCompare3TypesOrder() {
        assert(tc.compare3TypesOrder(null, doubleType, varcharType) == null);
        assert(tc.compare3TypesOrder(intType, null, varcharType) == null);
        assert(tc.compare3TypesOrder(intType, doubleType, null) == null);
        assert(tc.compare3TypesOrder(intType, intType, intType) == null);
        assert(tc.compare3TypesOrder(doubleType, doubleType, doubleType) == null);
        assert(tc.compare3TypesOrder(varcharType, varcharType, varcharType) == null);
        assert(tc.compare3TypesOrder(intType, doubleType, varcharType) == varcharType);
        assert(tc.compare3TypesOrder(varcharType, varcharType, intType) == varcharType);
        assert(tc.compare3TypesOrder(varcharType, intType, intType) == varcharType);
        assert(tc.compare3TypesOrder(intType, varcharType, intType) == varcharType);
        assert(tc.compare3TypesOrder(varcharType, doubleType, intType) == varcharType);
        assert(tc.compare3TypesOrder(varcharType, timestampType, intType) == varcharType);
        assert(tc.compare3TypesOrder(dateType, doubleType, varcharType) == varcharType);

    }
}
