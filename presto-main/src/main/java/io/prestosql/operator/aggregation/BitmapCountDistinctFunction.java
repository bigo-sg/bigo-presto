package io.prestosql.operator.aggregation;

import io.prestosql.operator.aggregation.state.Bitmap64State;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.type.BigintType.BIGINT;

@Description("Using bitmap to calculate count distinct value for int/bigint type column.")
@AggregationFunction("bitmap_count_distinct")
public final class BitmapCountDistinctFunction {
    private BitmapCountDistinctFunction()
    {
    }

    @InputFunction
    public static void input(Bitmap64State state, @SqlType(StandardTypes.BIGINT) long value) {
        state.get().add(value);
    }

    @CombineFunction
    public static void combine(Bitmap64State state, Bitmap64State otherState) {
        if (otherState != null) {
            state.get().or(otherState.get());
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(Bitmap64State state, BlockBuilder out) {
        if (state.get() == null) {
            out.appendNull();
        } else {
            BIGINT.writeLong(out, state.get().size());
            out.closeEntry();
        }
    }
}
