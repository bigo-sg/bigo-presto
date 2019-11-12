package io.prestosql.operator.aggregation.bitmap64;

import org.roaringbitmap.longlong.LongIterator;

/**
 * This class is meant to represent a simple wrapper around an immutable bitmap
 * class.
 */
public interface LongImmutableBitmap {
    /**
     * @return an iterator over the set bits of this bitmap
     */
    LongIterator iterator();

    /**
     * @return The number of bits set to true in this bitmap
     */
    long size();

    byte[] toBytes();

    /**
     * @return True if this bitmap is empty (contains no set bit)
     */
    boolean isEmpty();

    /**
     * Returns true if the bit at position value is set
     *
     * @param value the position to check
     * @return true if bit is set
     */
    boolean get(long value);
}
