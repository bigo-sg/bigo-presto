package io.prestosql.operator.aggregation.bitmap64;

import java.nio.ByteBuffer;

public interface LongBitmapFactory {
    LongMutableBitmap makeEmptyMutableBitmap();

    LongImmutableBitmap makeEmptyImmutableBitmap();

    LongImmutableBitmap makeImmutableBitmap(LongMutableBitmap mutableBitmap);

    LongMutableBitmap mapMutableBitmap(ByteBuffer b);
}

