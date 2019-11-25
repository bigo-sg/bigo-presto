/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.analyzer;

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_IN_DISTINCT;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_AGGREGATE;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;

@Test(singleThreaded = true)
public class TestTypeConversion extends AbstractTestTypeConversion {
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
        assert (tc.compare2TypesOrder(decimalType, decimalType) == null);
        assert (tc.compare2TypesOrder(intType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(doubleType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(timestampType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(dateType, varcharType) == varcharType);
        assert (tc.compare2TypesOrder(timestampType, timestampType) == null);
        assert (tc.compare2TypesOrder(dateType, dateType) == null);
        assert (tc.compare2TypesOrder(dateType, doubleType) == null);
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

    @Test
    public void teststringAndValueType() {
        assert (tc.stringAndValueType(doubleType, varcharType) == varcharType);
        assert (tc.stringAndValueType(varcharType, doubleType) == varcharType);
        assert (tc.stringAndValueType(doubleType, doubleType) == null);
        assert (tc.stringAndValueType(intType, intType) == null);
        assert (tc.stringAndValueType(null, intType) == null);
        assert (tc.stringAndValueType(null, varcharType) == null);
    }

    @Test
    public void testComparisonForBoolean()
    {
        analyzeHive("select 1=true");
        analyzeHive("select 1!=false");
        analyzeHive("select 0>true");
        analyzeHive("select 0<true");
        analyzeHive("select 1>=false");
        analyzeHive("select 1<false");
    }
}
