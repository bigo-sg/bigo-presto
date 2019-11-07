package io.prestosql.operator.aggregation.state;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.bitmap64.LongBitmapFactory;
import io.prestosql.operator.aggregation.bitmap64.LongMutableBitmap;
import io.prestosql.operator.aggregation.bitmap64.LongRoaringBitmapFactory;
import io.prestosql.spi.function.AccumulatorStateFactory;

import static java.util.Objects.requireNonNull;

public class LongRoaringBitmapStateFactory implements AccumulatorStateFactory<Bitmap64State> {

    private static final LongBitmapFactory factory = new LongRoaringBitmapFactory();

    @Override
    public Bitmap64State createSingleState() {
        SingleState state = new SingleState();
        state.set(factory.makeEmptyMutableBitmap());

        return state;
    }

    @Override
    public Class<? extends Bitmap64State> getSingleStateClass() {
        return SingleState.class;
    }

    @Override
    public Bitmap64State createGroupedState() {
        GroupedState state = new GroupedState();
        state.set(factory.makeEmptyMutableBitmap());

        return state;
    }

    @Override
    public Class<? extends Bitmap64State> getGroupedStateClass() {
        return GroupedState.class;
    }

    public static class SingleState implements Bitmap64State {
        private LongMutableBitmap bitmap;

        @Override
        public long getEstimatedSize() {
            return bitmap.getSizeInBytes();
        }

        @Override
        public void set(LongMutableBitmap bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public LongMutableBitmap get() {
            return bitmap;
        }
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState implements Bitmap64State {

        private final ObjectBigArray<LongMutableBitmap> bitmaps = new ObjectBigArray<>();
        private long size;

        @Override
        public void set(LongMutableBitmap value) {
            requireNonNull(value, "value is null");

            LongMutableBitmap previous = get();
            if (previous != null) {
                size -= previous.getSizeInBytes();
            }

            bitmaps.set(getGroupId(), value);
            size += value.getSizeInBytes();
        }

        @Override
        public LongMutableBitmap get() {
            LongMutableBitmap bitmap = bitmaps.get(getGroupId());
            if (bitmap == null) {
                bitmap = factory.makeEmptyMutableBitmap();

                bitmaps.set(getGroupId(), bitmap);
            }

            return bitmap;
        }

        @Override
        public void ensureCapacity(long size) {
            bitmaps.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize() {
            return size + bitmaps.sizeOf();
        }
    }

}
