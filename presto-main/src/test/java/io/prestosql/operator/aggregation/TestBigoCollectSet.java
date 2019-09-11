package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.aggregation.groupby.AggregationTestInput;
import io.prestosql.operator.aggregation.groupby.AggregationTestInputBuilder;
import io.prestosql.operator.aggregation.groupby.AggregationTestOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.Test;
import org.testng.internal.collections.Ints;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.prestosql.block.BlockAssertions.createArrayBigintBlock;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.block.BlockAssertions.createTypedLongsBlock;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertTrue;

public class TestBigoCollectSet {
    private static final Metadata metadata = createTestMetadataManager();

    @Test
    public void testEmpty() {
        InternalAggregationFunction bigIntAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                null,
                createLongsBlock(new Long[]{}));
    }

    @Test
    public void testNullOnly() {
        InternalAggregationFunction bigIntAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                null,
                createLongsBlock(new Long[]{null, null, null}));
    }

    @Test
    public void testNullPartial() {
        InternalAggregationFunction bigIntAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                Arrays.asList(2L, 5L),
                createLongsBlock(new Long[]{null, 2L, null, 5L, null}));
    }

    @Test
    public void testBoolean() {
        InternalAggregationFunction booleanAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(boolean)"), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                booleanAgg,
                Arrays.asList(true, false),
                createBooleansBlock(new Boolean[]{true, false}));
    }

    @Test
    public void testBigInt() {
        InternalAggregationFunction bigIntAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                Arrays.asList(3L, 1L),
                createLongsBlock(new Long[]{3L, 1L, 3L, 3L}));
    }

    @Test
    public void testDouble() {
        InternalAggregationFunction bigIntAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(double)"), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                bigIntAgg,
                Arrays.asList(3.0, 1.0),
                createDoublesBlock(3.0, 1.0, 3.0, 3.0));
    }

    @Test
    public void testVarchar() {
        InternalAggregationFunction varcharAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(varchar)"), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                varcharAgg,
                Arrays.asList("hello", "world"),
                createStringsBlock(new String[]{"hello", "world", "hello", "hello"}));
    }

    @Test
    public void testDate() {
        InternalAggregationFunction varcharAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(date)"), parseTypeSignature(StandardTypes.DATE)));
        assertAggregation(
                varcharAgg,
                Arrays.asList(new SqlDate(1), new SqlDate(2), new SqlDate(4)),
                createTypedLongsBlock(DATE, ImmutableList.of(1L, 2L, 4L)));
    }

    @Test
    public void testArray() {
        InternalAggregationFunction varcharAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(array(bigint))"), parseTypeSignature("array(bigint)")));
        assertAggregation(
                varcharAgg,
                Arrays.asList(Arrays.asList(1L), Arrays.asList(1L, 2L), Arrays.asList(1L, 2L, 3L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(1L, 2L), ImmutableList.of(1L, 2L, 3L))));
    }

    @Test
    public void testEmptyStateOutputsNull() {
        InternalAggregationFunction bigIntAgg = metadata.getAggregateFunctionImplementation(
                new Signature("collect_set", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        GroupedAccumulator groupedAccumulator = bigIntAgg.bind(Ints.asList(new int[]{}), Optional.empty())
                .createGroupedAccumulator();
        BlockBuilder blockBuilder = groupedAccumulator.getFinalType().createBlockBuilder(null, 1000);

        groupedAccumulator.evaluateFinal(0, blockBuilder);
        assertTrue(blockBuilder.isNull(0));
    }

    @Test
    public void testWithMultiplePages() {
        InternalAggregationFunction varcharAgg = metadata.getAggregateFunctionImplementation(
                new Signature(
                        "collect_set",
                        AGGREGATE,
                        parseTypeSignature("array(varchar)"),
                        parseTypeSignature(StandardTypes.VARCHAR)));

        AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                new Block[]{
                        createStringsBlock("hello", "world", "hello2", "world2", "hello3", "world3", "goodbye")},
                varcharAgg);
        AggregationTestOutput testOutput = new AggregationTestOutput(ImmutableList.of("hello", "world", "hello2", "world2", "hello3", "world3", "goodbye"));
        AggregationTestInput testInput = testInputBuilder.build();

        testInput.runPagesOnAccumulatorWithAssertion(0L, testInput.createGroupedAccumulator(), testOutput);
    }

    @Test
    public void testMultipleGroupsWithMultiplePages() {
        InternalAggregationFunction varcharAgg = metadata.getAggregateFunctionImplementation(
                new Signature(
                        "collect_set",
                        AGGREGATE,
                        parseTypeSignature("array(varchar)"),
                        parseTypeSignature(StandardTypes.VARCHAR)));

        Block block1 = createStringsBlock("a", "b", "c", "d", "e");
        Block block2 = createStringsBlock("f", "g", "h", "i", "j");
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableList.of("a", "b", "c", "d", "e"));
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[]{block1},
                varcharAgg);
        AggregationTestInput test1 = testInputBuilder1.build();
        GroupedAccumulator groupedAccumulator = test1.createGroupedAccumulator();

        test1.runPagesOnAccumulatorWithAssertion(0L, groupedAccumulator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableList.of("f", "g", "h", "i", "j"));
        AggregationTestInputBuilder testBuilder2 = new AggregationTestInputBuilder(
                new Block[]{block2},
                varcharAgg);
        AggregationTestInput test2 = testBuilder2.build();
        test2.runPagesOnAccumulatorWithAssertion(255L, groupedAccumulator, aggregationTestOutput2);
    }

    @Test
    public void testManyValues() {
        InternalAggregationFunction varcharAgg = metadata.getAggregateFunctionImplementation(
                new Signature(
                        "collect_set",
                        AGGREGATE,
                        parseTypeSignature("array(varchar)"),
                        parseTypeSignature(StandardTypes.VARCHAR)));

        int numGroups = 5;
        int arraySize = 200;
        Random random = new Random();

        for (int j = 0; j < numGroups; j++) {
            List<String> expectedValues = new ArrayList<>();
            List<String> valueList = new ArrayList<>();

            for (int i = 0; i < arraySize; i++) {
                String str = String.valueOf(random.nextInt(100));
                valueList.add(str);
                if (!expectedValues.contains(str)) {
                    expectedValues.add(str);
                }
            }

            assertAggregation(
                    varcharAgg,
                    expectedValues,
                    createStringsBlock(valueList));
        }
    }
}
