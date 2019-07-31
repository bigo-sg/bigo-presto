package io.prestosql.sql.analyzer;

import io.prestosql.spi.type.Type;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.slice.SliceUtf8;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.scalar.FormatFunction;
import io.prestosql.security.AccessControl;
import io.prestosql.security.DenyAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.*;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AtTimeZone;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BindExpression;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CurrentPath;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.CurrentUser;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Extract;
import io.prestosql.sql.tree.FieldReference;
import io.prestosql.sql.tree.Format;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.StackableAstVisitor;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.tree.WhenClause;
import io.prestosql.sql.tree.WindowFrame;
import io.prestosql.type.DecimalParametricType;
import io.prestosql.type.FunctionType;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.function.OperatorType.SUBSCRIPT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.prestosql.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static io.prestosql.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SUBQUERY;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.STANDALONE_LAMBDA;
import static io.prestosql.sql.analyzer.SemanticErrorCode.TOO_MANY_ARGUMENTS;
import static io.prestosql.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static io.prestosql.sql.analyzer.SemanticExceptions.missingAttributeException;
import static io.prestosql.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static io.prestosql.sql.tree.Extract.Field.TIMEZONE_HOUR;
import static io.prestosql.sql.tree.Extract.Field.TIMEZONE_MINUTE;
import static io.prestosql.type.ArrayParametricType.ARRAY;
import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.DateTimeUtils.parseTimestampLiteral;
import static io.prestosql.util.DateTimeUtils.timeHasTimeZone;
import static io.prestosql.util.DateTimeUtils.timestampHasTimeZone;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class TypeConversion {
    private TypeConversion() {
    }

    private boolean canConvertType(Type leftType, Type rightType){
        String leftTypeName = leftType.getDisplayName();
        String rightTypeName = rightType.getDisplayName();
        List<String> booleanConvertList = Collections.singletonList("boolean");
        List<String> tinyintConvertList =Arrays.asList("tinyint","smallint", "integer", "bigint", "double", "decimal",
                "varchar");
        List<String> smallintConvertList =Arrays.asList("smallint", "integer", "bigint", "double", "decimal", "varchar");
        List<String> integerConvertList =Arrays.asList("integer", "bigint", "double", "decimal", "varchar");
        List<String> bigintConvertList =Arrays.asList("bigint", "double", "decimal", "varchar");
        List<String> doubleConvertList =Arrays.asList("double", "decimal", "varchar");
        List<String> decimalConvertList =Arrays.asList("decimal", "varchar");
        List<String> varcharConvertList =Arrays.asList("double", "decimal", "varchar");
        List<String> timestampConvertList =Arrays.asList("varchar", "timestamp");
        List<String> dateConvertList =Arrays.asList("varchar", "date");
        List<String> varbinaryConvertList = Collections.singletonList("varbinary");

        switch(leftTypeName) {
            case "boolean":
                return booleanConvertList.contains(rightTypeName);
            case "tinyint":
                return tinyintConvertList.contains(rightTypeName);
            case "smallint":
                return smallintConvertList.contains(rightTypeName);
            case "integer":
                return integerConvertList.contains(rightTypeName);
            case "bigint":
                return bigintConvertList.contains(rightTypeName);
            case "double":
                return doubleConvertList.contains(rightTypeName);
            case "decimal":
                return decimalConvertList.contains(rightTypeName);
            case "varchar":
                return varcharConvertList.contains(rightTypeName);
            case "timestamp":
                return timestampConvertList.contains(rightTypeName);
            case "date":
                return dateConvertList.contains(rightTypeName);
            case "varbinary":
                return varbinaryConvertList.contains(rightTypeName);
            default:
                return false;
        }
    }

    private boolean needConvert(Type leftType, Type rightType){
        if(leftType.getDisplayName().equals(rightType.getDisplayName())){
            return false;
        }
        return true;
    }

    private String convertType(Type leftType, Type rightType){

        return leftType.getDisplayName();
    }

    public static void main(String[] args) {
        TypeConversion tc = new TypeConversion();
        System.out.println(tc.canConvertType(DATE, DOUBLE));
        System.out.println(tc.canConvertType(INTEGER, DOUBLE));

    }

}
