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

    protected boolean canConvertType(Type leftType, Type rightType) {

        String leftTypeName = leftType.getTypeSignature().getBase();
        String rightTypeName = rightType.getTypeSignature().getBase();

        List<String> booleanConvertList = Collections.singletonList(StandardTypes.BOOLEAN);
        List<String> tinyintConvertList = Arrays.asList(StandardTypes.TINYINT, StandardTypes.SMALLINT,
                StandardTypes.INTEGER, StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL,
                StandardTypes.VARCHAR);
        List<String> smallintConvertList = Arrays.asList(StandardTypes.SMALLINT, StandardTypes.INTEGER,
                StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> integerConvertList = Arrays.asList(StandardTypes.INTEGER,
                StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> bigintConvertList = Arrays.asList(StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL,
                StandardTypes.VARCHAR);
        List<String> doubleConvertList = Arrays.asList(StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> decimalConvertList = Arrays.asList(StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> varcharConvertList = Arrays.asList(StandardTypes.DOUBLE, StandardTypes.DECIMAL, StandardTypes.VARCHAR);
        List<String> timestampConvertList = Arrays.asList(StandardTypes.VARCHAR, StandardTypes.TIMESTAMP);
        List<String> dateConvertList = Arrays.asList(StandardTypes.VARCHAR, StandardTypes.DATE);

        switch (leftTypeName) {
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

    protected boolean needConvert(Type leftType, Type rightType) {
        //todo
        if(leftType.getDisplayName().equals(rightType.getDisplayName())){
            return false;
        }
        return true;
    }

    protected boolean isValueType(Type type) {
        if (type == null) {
            return false;
        }
        List<String> valueTypes = Arrays.asList(StandardTypes.TINYINT, StandardTypes.SMALLINT, StandardTypes.INTEGER,
                StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.DECIMAL);
        return valueTypes.contains(type.getTypeSignature().getBase());
    }

    protected Type stringAndValueType(Type leftType, Type rightType) {
        if (leftType == null || rightType == null) {
            return null;
        }
        if (typeConvertOrderMap.get(leftType.getTypeSignature().getBase()) == null
                || typeConvertOrderMap.get(rightType.getTypeSignature().getBase()) == null) {
            return null;
        }
        List<String> valueTypes = Arrays.asList(StandardTypes.DOUBLE, StandardTypes.TINYINT, StandardTypes.SMALLINT,
                StandardTypes.INTEGER, StandardTypes.BIGINT, StandardTypes.DECIMAL);
        String leftTypeName = leftType.getTypeSignature().getBase();
        String rightTypeName = rightType.getTypeSignature().getBase();

        if (leftTypeName.equals(StandardTypes.VARCHAR) && valueTypes.contains(rightTypeName)) {
            return leftType;
        } else if (rightTypeName.equals(StandardTypes.VARCHAR) && valueTypes.contains(leftTypeName)) {
            return rightType;
        }
        return null;
    }

    protected Type compare2TypesOrder(Type leftType, Type rightType) {
        if (leftType == null || rightType == null) {
            return null;
        }
        if (typeConvertOrderMap.get(leftType.getTypeSignature().getBase()) == null
                || typeConvertOrderMap.get(rightType.getTypeSignature().getBase()) == null) {
            return null;
        }
        if (isValueType(leftType) && isValueType(rightType)) {
            return null;
        }
        int leftOrder = typeConvertOrderMap.get(leftType.getTypeSignature().getBase());
        int rightOrder = typeConvertOrderMap.get(rightType.getTypeSignature().getBase());

        if(leftOrder == rightOrder){
            return null;
        }
        return leftOrder >= rightOrder ? leftType : rightType;
    }

    protected Type compare3TypesOrder(Type leftType, Type middleType, Type rightType) {
        if (leftType == null || middleType == null || rightType == null) {
            return null;
        }
        if (typeConvertOrderMap.get(leftType.getTypeSignature().getBase()) == null
                || typeConvertOrderMap.get(middleType.getTypeSignature().getBase()) == null
                || typeConvertOrderMap.get(rightType.getTypeSignature().getBase()) == null){
            return null;
        }
        int leftOrder = typeConvertOrderMap.get(leftType.getTypeSignature().getBase());
        int middleOrder = typeConvertOrderMap.get(middleType.getTypeSignature().getBase());
        int rightOrder = typeConvertOrderMap.get(rightType.getTypeSignature().getBase());
        int maxOrder = Math.max(Math.max(leftOrder, middleOrder), rightOrder);

        if(maxOrder == leftOrder){
            if(canConvertType(middleType, leftType) && canConvertType(rightType, leftType)){
                return leftType;
            }else{
                Type tmpType = compare2TypesOrder(middleType, rightType);
                if(tmpType == middleType && canConvertType(leftType, middleType) && canConvertType(rightType, middleType)){
                    return middleType;
                }else if(tmpType == middleType && canConvertType(leftType, rightType) && canConvertType(middleType, rightType)){
                    return rightType;
                }
                if(tmpType == rightType && canConvertType(leftType, rightType) && canConvertType(middleType, rightType)){
                    return rightType;
                }else if(tmpType == rightType && canConvertType(leftType, middleType) && canConvertType(rightType, middleType)){
                    return middleType;
                }
            }
        }else if (maxOrder == middleOrder){
            if(canConvertType(leftType, middleType) && canConvertType(rightType, middleType)){
                return middleType;
            }else{
                Type tmpType = compare2TypesOrder(leftType, rightType);
                if(tmpType == leftType && canConvertType(middleType, leftType) && canConvertType(rightType, leftType)){
                    return leftType;
                }else if(tmpType == leftType && canConvertType(middleType, rightType) && canConvertType(leftType, rightType)){
                    return rightType;
                }
                if(tmpType == rightType && canConvertType(leftType, rightType) && canConvertType(middleType, rightType)){
                    return rightType;
                }else if(tmpType == rightType && canConvertType(rightType, leftType) && canConvertType(middleType, leftType)){
                    return leftType;
                }
            }
        }else{
            if(canConvertType(middleType, rightType) && canConvertType(leftType, rightType)){
                return rightType;
            }else{
                Type tmpType = compare2TypesOrder(middleType, rightType);
                if(tmpType == middleType && canConvertType(leftType, middleType) && canConvertType(rightType, middleType)){
                    return middleType;
                }else if(tmpType == middleType && canConvertType(middleType, leftType) && canConvertType(rightType, leftType)){
                    return leftType;
                }
                if(tmpType == leftType && canConvertType(rightType, leftType) && canConvertType(middleType, leftType)){
                    return leftType;
                }else if(tmpType == leftType && canConvertType(rightType, middleType) && canConvertType(leftType, middleType)){
                    return middleType;
                }
            }
        }
        return null;
    }
}
