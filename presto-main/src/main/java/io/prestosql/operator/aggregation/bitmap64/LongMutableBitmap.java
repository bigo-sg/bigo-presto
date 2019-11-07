package io.prestosql.operator.aggregation.bitmap64;

/**
 * This class is meant to represent a simple wrapper around a bitmap class.
 */
public interface LongMutableBitmap extends LongImmutableBitmap {
    /**
     * Empties the content of this bitmap.
     */
    void clear();

    /**
     * Compute the bitwise-or of this bitmap with another bitmap. The current
     * bitmap is modified whereas the other bitmap is left intact.
     * <p>
     * Note that the other bitmap should be of the same class instance.
     *
     * @param mutableBitmap other bitmap
     */
    void or(LongMutableBitmap mutableBitmap);

    /**
     * Compute the bitwise-and of this bitmap with another bitmap. The current
     * bitmap is modified whereas the other bitmap is left intact.
     * <p>
     * Note that the other bitmap should be of the same class instance.
     *
     * @param mutableBitmap other bitmap
     */
    void and(LongMutableBitmap mutableBitmap);


    /**
     * Compute the bitwise-xor of this bitmap with another bitmap. The current
     * bitmap is modified whereas the other bitmap is left intact.
     * <p>
     * Note that the other bitmap should be of the same class instance.
     *
     * @param mutableBitmap other bitmap
     */
    void xor(LongMutableBitmap mutableBitmap);

    /**
     * Compute the bitwise-andNot of this bitmap with another bitmap. The current
     * bitmap is modified whereas the other bitmap is left intact.
     * <p>
     * Note that the other bitmap should be of the same class instance.
     *
     * @param mutableBitmap other bitmap
     */
    void andNot(LongMutableBitmap mutableBitmap);

    /**
     * Return the size in bytes for the purpose of serialization to a ByteBuffer.
     * Note that this is distinct from the memory usage.
     *
     * @return the total set in bytes
     */
    long getSizeInBytes();

    /**
     * Add the specified integer to the bitmap. This is equivalent to setting the
     * ith bit to the value 1.
     *
     * @param entry integer to be added
     */
    void add(long entry);

    /**
     * Remove the specified integer to the bitmap. This is equivalent to setting the
     * ith bit to the value 1.
     *
     * @param entry integer to be remove
     */
    void remove(long entry);

}
