package io.prestosql.operator.aggregation.bitmap64;

import com.google.common.base.Throwables;

import java.nio.ByteBuffer;

public class LongRoaringBitmapFactory implements LongBitmapFactory {
    private static final LongImmutableRoaringBitmap EMPTY_IMMUTABLE_BITMAP = new LongImmutableRoaringBitmap();

    @Override
    public LongMutableBitmap makeEmptyMutableBitmap() {
        return new LongRoaringBitmap();
    }

    @Override
    public LongImmutableBitmap makeEmptyImmutableBitmap() {
        return EMPTY_IMMUTABLE_BITMAP;
    }

    @Override
    public LongImmutableBitmap makeImmutableBitmap(LongMutableBitmap mutableBitmap) {
        if (!(mutableBitmap instanceof LongRoaringBitmap)) {
            throw new IllegalStateException("Cannot convert [%s]" + mutableBitmap.getClass());
        }
        try {
            return ((LongRoaringBitmap) mutableBitmap).toImmutableBitmap();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public LongMutableBitmap mapMutableBitmap(ByteBuffer b) {
        return LongRoaringBitmap.deserialize(b.array());
    }
}