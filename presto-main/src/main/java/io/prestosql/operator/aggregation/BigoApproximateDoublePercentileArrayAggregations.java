package io.prestosql.operator.aggregation;

import io.airlift.stats.QuantileDigest;
import io.prestosql.operator.aggregation.state.DigestAndPercentileArrayState;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.StandardTypes;

import java.util.List;

import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

@AggregationFunction("percentile_approx")
public final class BigoApproximateDoublePercentileArrayAggregations
{
    private BigoApproximateDoublePercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.input(state, doubleToSortableLong(value), percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.weightedInput(state, doubleToSortableLong(value), weight, percentilesArrayBlock);
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
    {
        ApproximateLongPercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction("array(double)")
    public static void output(@AggregationState DigestAndPercentileArrayState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        for (int i = 0; i < percentiles.size(); i++) {
            Double percentile = percentiles.get(i);
            DOUBLE.writeDouble(blockBuilder, sortableLongToDouble(digest.getQuantile(percentile)));
        }

        out.closeEntry();
    }
}
