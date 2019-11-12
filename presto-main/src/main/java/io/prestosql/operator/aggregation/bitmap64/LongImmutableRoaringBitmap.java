package io.prestosql.operator.aggregation.bitmap64;

import com.google.common.base.Throwables;
import org.roaringbitmap.BitmapDataProviderSupplier;
import org.roaringbitmap.RoaringBitmapSupplier;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class LongImmutableRoaringBitmap implements LongImmutableBitmap {
    protected InnerRoaringBitmap64 underlyingBitmap;

    public LongImmutableRoaringBitmap() {
        this(new InnerRoaringBitmap64());
    }

    public LongImmutableRoaringBitmap(InnerRoaringBitmap64 underlyingBitmap) {
        this.underlyingBitmap = underlyingBitmap;
    }

    @Override
    public LongIterator iterator() {
        return underlyingBitmap.getLongIterator();
    }

    @Override
    public long size() {
        return underlyingBitmap.getLongCardinality();
    }

    @Override
    public byte[] toBytes() {
        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            underlyingBitmap.serialize(new DataOutputStream(out));
            return out.toByteArray();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return underlyingBitmap.isEmpty();
    }

    @Override
    public boolean get(long value) {
        return underlyingBitmap.contains(value);
    }

    static class InnerRoaringBitmap64 extends Roaring64NavigableMap {
        private static BitmapDataProviderSupplier supplier = new RoaringBitmapSupplier();

        public InnerRoaringBitmap64() {
            super(supplier);
        }

        public InnerRoaringBitmap64 copy() {
            InnerRoaringBitmap64 copy = new InnerRoaringBitmap64();
            LongIterator iter = getLongIterator();
            while (iter.hasNext()) {
                copy.addLong(iter.next());
            }
            return copy;
        }
    }
}
