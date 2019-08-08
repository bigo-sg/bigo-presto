package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.*;

import java.util.*;

public class TypeConversion {
    TypeConversion() {
    }

    private static final Map<String, Integer> typeConvertOrderMap = ImmutableMap.<String, Integer>builder()
            .put(StandardTypes.BOOLEAN, 1)
            .put(StandardTypes.TINYINT, 2)
            .put(StandardTypes.SMALLINT, 3)
            .put(StandardTypes.INTEGER, 4)
            .put(StandardTypes.BIGINT, 5)
            .put(StandardTypes.DOUBLE, 6)
            .put(StandardTypes.DECIMAL, 7)
            .put(StandardTypes.VARCHAR, 8)
            .put(StandardTypes.TIMESTAMP, 9)
            .put(StandardTypes.DATE, 10)
            .build();

    protected boolean canConvertType (Type leftType, Type rightType) {
        String leftTypeName = leftType.getTypeSignature().getBase();
        String rightTypeName = rightType.getTypeSignature().getBase();

        List<String> booleanConvertList = Collections.singletonList(StandardTypes.BOOLEAN);
        List<String> tinyintConvertList =Arrays.asList(StandardTypes.TINYINT, StandardTypes.SMALLINT,
                StandardTypes.INTEGER, StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL,
                StandardTypes.VARCHAR);
        List<String> smallintConvertList =Arrays.asList(StandardTypes.SMALLINT, StandardTypes.INTEGER,
                StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> integerConvertList =Arrays.asList(StandardTypes.INTEGER,
                StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> bigintConvertList =Arrays.asList(StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL,
                StandardTypes.VARCHAR);
        List<String> doubleConvertList =Arrays.asList(StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> decimalConvertList =Arrays.asList(StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> varcharConvertList =Arrays.asList(StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> timestampConvertList =Arrays.asList(StandardTypes.VARCHAR, StandardTypes.TIMESTAMP);
        List<String> dateConvertList =Arrays.asList(StandardTypes.VARCHAR, StandardTypes.DATE);

        switch(leftTypeName) {
            case StandardTypes.BOOLEAN:
                return booleanConvertList.contains(rightTypeName);
            case StandardTypes.TINYINT:
                return tinyintConvertList.contains(rightTypeName);
            case StandardTypes.SMALLINT:
                return smallintConvertList.contains(rightTypeName);
            case StandardTypes.INTEGER:
                return integerConvertList.contains(rightTypeName);
            case StandardTypes.BIGINT:
                return bigintConvertList.contains(rightTypeName);
            case StandardTypes.DOUBLE:
                return doubleConvertList.contains(rightTypeName);
            case StandardTypes.DECIMAL:
                return decimalConvertList.contains(rightTypeName);
            case StandardTypes.VARCHAR:
                return varcharConvertList.contains(rightTypeName);
            case StandardTypes.TIMESTAMP:
                return timestampConvertList.contains(rightTypeName);
            case StandardTypes.DATE:
                return dateConvertList.contains(rightTypeName);
            default:
                return false;
        }
    }

    protected boolean needConvert(Type leftType, Type rightType){
        //todo
        if (leftType.getDisplayName().equals(rightType.getDisplayName())) {
            return false;
        }
        return true;
    }

    protected Type compare2TypesOrder(Type leftType, Type rightType){
        if (typeConvertOrderMap.get(leftType.getTypeSignature().getBase()) == null
                || typeConvertOrderMap.get(rightType.getTypeSignature().getBase()) == null) {
            return null;
        }
        int leftOrder = typeConvertOrderMap.get(leftType.getTypeSignature().getBase());
        int rightOrder = typeConvertOrderMap.get(rightType.getTypeSignature().getBase());

        if (leftOrder > rightOrder && canConvertType(rightType, leftType)) {
            return leftType;
        } else if (leftOrder > rightOrder && canConvertType(leftType, rightType)) {
            return rightType;
        } else if (rightOrder > leftOrder && canConvertType(leftType, rightType)) {
            return rightType;
        } else if (rightOrder > leftOrder && canConvertType(rightType, leftType)) {
            return leftType;
        } else {
            return null;
        }
    }

    protected Type compare3TypesOrder (Type leftType, Type middleType, Type rightType) {
        if (typeConvertOrderMap.get(leftType.getTypeSignature().getBase()) == null
                || typeConvertOrderMap.get(middleType.getTypeSignature().getBase()) == null
                || typeConvertOrderMap.get(rightType.getTypeSignature().getBase()) == null) {
            return null;
        }
        int leftOrder = typeConvertOrderMap.get(leftType.getTypeSignature().getBase());
        int middleOrder = typeConvertOrderMap.get(middleType.getTypeSignature().getBase());
        int rightOrder = typeConvertOrderMap.get(rightType.getTypeSignature().getBase());

        if (leftOrder == middleOrder && middleOrder == rightOrder) {
            return null;
        }
        int maxOrder = Math.max(Math.max(leftOrder, middleOrder), rightOrder);

        if (maxOrder == leftOrder) {
            if (canConvertType(middleType, leftType) && canConvertType(rightType, leftType)) {
                return leftType;
            } else {
                if (middleOrder == rightOrder && canConvertType(leftType, middleType)) {
                    return middleType;
                }

                Type tmpType = compare2TypesOrder(middleType, rightType);
                if(tmpType == middleType && canConvertType(leftType, middleType)) {
                    return middleType;
                }
                if (tmpType == rightType && canConvertType(leftType, rightType)) {
                    return rightType;
                }
            }
        } else if (maxOrder == middleOrder) {
            if (canConvertType(leftType, middleType) && canConvertType(rightType, middleType)) {
                return middleType;
            } else {
                if (leftOrder == rightOrder && canConvertType(middleType, rightType)) {
                    return rightType;
                }

                Type tmpType = compare2TypesOrder(leftType, rightType);
                if (tmpType == leftType && canConvertType(middleType, leftType)) {
                    return leftType;
                }
                if (tmpType == rightType && canConvertType(middleType, rightType)) {
                    return rightType;
                }
            }
        } else {
            if (canConvertType(middleType, rightType) && canConvertType(leftType, rightType)) {
                return rightType;
            } else {
                if (leftOrder == middleOrder && canConvertType(rightType, leftType)) {
                    return leftType;
                }

                Type tmpType = compare2TypesOrder(leftType, middleType);
                if (tmpType == middleType && canConvertType(rightType, middleType)) {
                    return middleType;
                }
                if (tmpType == leftType && canConvertType(rightType, leftType)) {
                    return leftType;
                }
            }
        }
        return null;
    }
}