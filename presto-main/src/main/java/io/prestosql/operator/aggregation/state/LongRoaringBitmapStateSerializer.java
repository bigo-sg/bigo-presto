package io.prestosql.operator.aggregation.state;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.prestosql.operator.aggregation.bitmap64.LongRoaringBitmap;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class LongRoaringBitmapStateSerializer implements AccumulatorStateSerializer<Bitmap64State> {

    @Override
    public Type getSerializedType() {
        return VARCHAR;
    }

    @Override
    public void serialize(Bitmap64State state, BlockBuilder out) {
        LongRoaringBitmap bitmap = (LongRoaringBitmap) state.get();
        byte[] bytes = LongRoaringBitmap.serialize(bitmap);

        Slice slice = Slices.wrappedBuffer(bytes);

        out.writeInt(slice.length());
        out.writeBytes(slice, 0, bytes.length);

        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, Bitmap64State state) {
        SliceInput input = block.getSlice(index, 0, block.getSliceLength(index)).getInput();

        int length = input.readInt();
        LongRoaringBitmap longRoaringBitmap = LongRoaringBitmap.deserialize(input.readSlice(length).getBytes());

        state.set(longRoaringBitmap);
    }
}
