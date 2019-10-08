package io.prestosql.operator.aggregation.arrayagg;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class BigoCollectSetFunction
        extends SqlAggregationFunction {
    private static final String NAME = "collect_set";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(BigoCollectSetFunction.class, "input", Type.class, ArrayAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(BigoCollectSetFunction.class, "combine", Type.class, ArrayAggregationState.class, ArrayAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(BigoCollectSetFunction.class, "output", Type.class, ArrayAggregationState.class, BlockBuilder.class);

    private final ArrayAggGroupImplementation groupMode;

    public BigoCollectSetFunction(ArrayAggGroupImplementation groupMode) {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("T")));
        this.groupMode = requireNonNull(groupMode, "groupMode is null");
    }

    @Override
    public String getDescription() {
        return "return an array of values";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, Metadata metadata) {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type, groupMode);
    }

    private static InternalAggregationFunction generateAggregation(Type type, ArrayAggGroupImplementation groupMode) {
        DynamicClassLoader classLoader = new DynamicClassLoader(BigoCollectSetFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new ArrayAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ArrayAggregationStateFactory(type, groupMode);

        List<Type> inputTypes = ImmutableList.of(type);
        Type outputType = new ArrayType(type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);
        Class<? extends AccumulatorState> stateInterface = ArrayAggregationState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                Optional.empty(),
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value) {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, ArrayAggregationState state, Block value, int position) {
        if (!value.isNull(position)) {
            state.add(value, position);
        }
    }

    public static void combine(Type type, ArrayAggregationState state, ArrayAggregationState otherState) {
        state.merge(otherState);
    }

    public static void output(Type elementType, ArrayAggregationState state, BlockBuilder out) {
        if (state.isEmpty()) {
            out.appendNull();
        } else {
            BlockBuilder entryBuilder = out.beginBlockEntry();

            AtomicBoolean containsNull = new AtomicBoolean(false);
            AtomicInteger blockSize = new AtomicInteger();

            String blockSimpleName = state.getBlockSimpleName();
            state.forEach((block, position) -> blockSize.incrementAndGet());

            ObjectSet set = new ObjectLinkedOpenHashSet(blockSize.get());

            state.forEach((block, position) -> {
                if (block.isNull(position)) {
                    if (!containsNull.get()) {
                        containsNull.set(true);
                    }
                }
                if (!blockSimpleName.equals("group_state")) {
                    Object value = getValue(blockSimpleName, block, position);
                    if (!set.contains(value)) {
                        set.add(value);
                        elementType.appendTo(block, position, entryBuilder);
                    }
                } else {
                    GroupArrayAggregationState gs = (GroupArrayAggregationState) state;
                    String blockSimpleNameForGroup = gs.getBlockSimpleName(block);
                    Object value = getValue(blockSimpleNameForGroup, block, position);
                    if (!set.contains(value)) {
                        set.add(value);
                        elementType.appendTo(block, position, entryBuilder);
                    }
                }
            });
            out.closeEntry();
        }
    }

    public static Object getValue(String blockSimpleName, Block block, int position) {
        Object value = null;
        switch (blockSimpleName) {
            case "LongArrayBlockBuilder":
                value = BIGINT.getLong(block, position);
                break;
            case "IntArrayBlockBuilder":
                value = INTEGER.getLong(block, position);
                break;
            case "VariableWidthBlockBuilder":
                value = VARCHAR.getSlice(block, position);
                break;
            case "ByteArrayBlockBuilder":
                value = BOOLEAN.getBoolean(block, position);
                break;
            case "ArrayBlockBuilder":
            case "ArrayBlock":
                return block.getSingleValueBlock(position);
            default:
                throw new IllegalStateException("Unexpected value: " + blockSimpleName);
        }
        return value;
    }
}
