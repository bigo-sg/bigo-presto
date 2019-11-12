package io.prestosql.operator.aggregation.bitmap64;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class LongRoaringBitmap extends LongImmutableRoaringBitmap implements LongMutableBitmap {
    public LongRoaringBitmap() {
        super();
    }

    public LongRoaringBitmap(InnerRoaringBitmap64 underlyingBitmap) {
        super(underlyingBitmap);
    }

    @Override
    public void clear() {
        this.underlyingBitmap.clear();
    }

    @Override
    public void or(LongMutableBitmap mutableBitmap) {
        LongRoaringBitmap other = (LongRoaringBitmap) mutableBitmap;
        Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
        this.underlyingBitmap.or(unwrappedOtherBitmap);
    }

    @Override
    public void and(LongMutableBitmap mutableBitmap) {
        LongRoaringBitmap other = (LongRoaringBitmap) mutableBitmap;
        Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
        this.underlyingBitmap.and(unwrappedOtherBitmap);
    }

    @Override
    public void xor(LongMutableBitmap mutableBitmap) {
        LongRoaringBitmap other = (LongRoaringBitmap) mutableBitmap;
        Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
        this.underlyingBitmap.xor(unwrappedOtherBitmap);
    }

    @Override
    public void andNot(LongMutableBitmap mutableBitmap) {
        LongRoaringBitmap other = (LongRoaringBitmap) mutableBitmap;
        Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
        this.underlyingBitmap.andNot(unwrappedOtherBitmap);
    }

    @Override
    public long getSizeInBytes() {
        return this.underlyingBitmap.getLongSizeInBytes();
    }

    @Override
    public void add(long entry) {
        this.underlyingBitmap.add(entry);
    }

    @Override
    public void remove(long entry) {
        this.underlyingBitmap.removeLong(entry);
    }

    public LongImmutableRoaringBitmap toImmutableBitmap() {
        InnerRoaringBitmap64 mrb = this.underlyingBitmap.copy();
        return new LongImmutableRoaringBitmap(mrb);
    }

    //------serde-----

    public static LongRoaringBitmap deserialize(byte[] bytes) {
        InnerRoaringBitmap64 innerRoaringBitmap64 = new InnerRoaringBitmap64();
        try {
            innerRoaringBitmap64.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new LongRoaringBitmap(innerRoaringBitmap64);
    }

    public static byte[] serialize(LongRoaringBitmap longRoaringBitmap) {
        return longRoaringBitmap.toBytes();
    }
}