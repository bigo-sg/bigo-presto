package io.prestosql.operator.aggregation;

import io.airlift.stats.QuantileDigest;
import io.prestosql.operator.aggregation.state.DigestAndPercentileState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.StandardTypes;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("percentile_approx")
public class BigoApproximateRealPercentileAggregations
{
    private BigoApproximateRealPercentileAggregations() {}

    @InputFunction
    public static void input(@AggregationState DigestAndPercentileState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateLongPercentileAggregations.input(state, floatToSortableInt(intBitsToFloat((int) value)), percentile);
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateLongPercentileAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentile);
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        ApproximateLongPercentileAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentile, accuracy);
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileState state, @AggregationState DigestAndPercentileState otherState)
    {
        ApproximateLongPercentileAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.REAL)
    public static void output(@AggregationState DigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            REAL.writeLong(out, floatToRawIntBits(sortableIntToFloat((int) digest.getQuantile(percentile))));
        }
    }
}
