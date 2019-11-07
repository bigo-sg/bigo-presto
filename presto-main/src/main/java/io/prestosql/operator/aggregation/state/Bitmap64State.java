package io.prestosql.operator.aggregation.state;

import io.prestosql.operator.aggregation.bitmap64.LongMutableBitmap;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateSerializerClass = LongRoaringBitmapStateSerializer.class, stateFactoryClass = LongRoaringBitmapStateFactory.class)
public interface Bitmap64State extends AccumulatorState {
    void set(LongMutableBitmap bitmap);

    LongMutableBitmap get();
}
