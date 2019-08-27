package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Long.reverseBytes;
import static java.lang.Long.rotateRight;

public class BigoIndigoUidHash {

    static final long K0 = 0xc3a5c85c97cb3127L;
    static final long K1 = 0xb492b66fbe98f273L;
    static final long K2 = 0x9ae16a3b2f90404fL;

    @Description("self build udf, indigoUidHash")
    @ScalarFunction("indigouidhash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice indigoUidHash(@SqlType(StandardTypes.VARCHAR) Slice str)
    {
        return utf8Slice(evaluate(str.toStringUtf8()));
    }

    @Description("self build udf, indigoUidHash")
    @ScalarFunction("indigouidhash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice indigoUidHashLong(@SqlType(StandardTypes.BIGINT) long l)
    {
        return utf8Slice(evaluate(Long.toString(l)));
    }

    static long shiftMix(long val) {
        return val ^ (val >>> 47);
    }

    static long hashLen16(long u, long v) {
        return hashLen16(u, v, K_MUL);
    }

    private static final long K_MUL = 0x9ddfea08eb382d69L;

    static long hashLen16(long u, long v, long mul) {
        long a = shiftMix((u ^ v) * mul);
        return shiftMix((v ^ a) * mul) * mul;
    }

    static long mul(long len) {
        return K2 + (len << 1);
    }

    static long hash1To3Bytes(int len, int firstByte, int midOrLastByte, int lastByte) {
        int y = firstByte + (midOrLastByte << 8);
        int z = len + (lastByte << 2);
        return shiftMix((((long) y) * K2) ^ (((long) z) * K0)) * K2;
    }

    static long hash4To7Bytes(long len, long first4Bytes, long last4Bytes) {
        long mul = mul(len);
        return hashLen16(len + (first4Bytes << 3), last4Bytes, mul);
    }

    static long hash8To16Bytes(long len, long first8Bytes, long last8Bytes) {
        long mul = mul(len);
        long a = first8Bytes + K2;
        long c = rotateRight(last8Bytes, 37) * mul + a;
        long d = (rotateRight(a, 25) + last8Bytes) * mul;
        return hashLen16(c, d, mul);
    }

    private static long toLongLE(byte[] b, int i) {
        return (((long)b[i+7] << 56) +
                ((long)(b[i+6] & 255) << 48) +
                ((long)(b[i+5] & 255) << 40) +
                ((long)(b[i+4] & 255) << 32) +
                ((long)(b[i+3] & 255) << 24) +
                ((b[i+2] & 255) << 16) +
                ((b[i+1] & 255) <<  8) +
                ((b[i+0] & 255) <<  0));
    }
    private  static int toIntLE(byte[] b, int i) {
        return (((b[i+3] & 255) << 24) + ((b[i+2] & 255) << 16) + ((b[i+1] & 255) << 8) + ((b[i+0] & 255) << 0));
    }

    private static long fetch64(byte[] s, long pos) {
        return toLongLE(s, (int)pos);
    }

    private static int fetch32(byte[] s, long pos) {
        return toIntLE(s, (int)pos);
    }

    long toLittleEndian(long v) {
        return v;
    }

    int toLittleEndian(int v) {
        return v;
    }

    static long hashLen0To16(byte[] in, long off, long len) {
        if (len >= 8L) {
            long a = fetch64(in, off);
            long b = fetch64(in, off + len - 8L);
            return hash8To16Bytes(len, a, b);
        } else if (len >= 4L) {
            long a = 0xffffffffL & fetch32(in, off);
            long b = 0xffffffffL & fetch32(in, off + len - 4L);
            return hash4To7Bytes(len, a, b);
        } else if (len > 0L) {
            int a = in[(int)off+0] & 0xFF;
            int b = in[(int)(off + (len >> 1))] & 0xFF;
            int c = in[(int)(off + len - 1L)] & 0xFF;
            return hash1To3Bytes((int) len, a, b, c);
        }
        return K2;
    }

    static long hashLen17To32(byte[] in, long off, long len) {
        long mul = mul(len);
        long a = fetch64(in, off) * K1;
        long b = fetch64(in, off + 8L);
        long c = fetch64(in, off + len - 8L) * mul;
        long d = fetch64(in, off + len - 16L) * K2;
        return hashLen16(rotateRight(a + b, 43) + rotateRight(c, 30) + d,
                a + rotateRight(b + K2, 18) + c, mul);
    }

    private static long cityHashLen33To64(byte[] in, long off, long len) {
        long mul = mul(len);
        long a = fetch64(in, off) * K2;
        long b = fetch64(in, off + 8L);
        long c = fetch64(in, off + len - 24L);
        long d = fetch64(in, off + len - 32L);
        long e = fetch64(in, off + 16L) * K2;
        long f = fetch64(in, off + 24L) * 9L;
        long g = fetch64(in, off + len - 8L);
        long h = fetch64(in, off + len - 16L) * mul;
        long u = rotateRight(a + g, 43) + (rotateRight(b, 30) + c) * 9L;
        long v = ((a + g) ^ d) + f + 1L;
        long w = reverseBytes((u + v) * mul) + h;
        long x = rotateRight(e + f, 42) + c;
        long y = (reverseBytes((v + w) * mul) + g) * mul;
        long z = e + f + c;
        a = reverseBytes((x + z) * mul + y) + b;
        b = shiftMix((z + a) * mul + d + h) * mul;
        return b + x;
    }

    public static long cityHash64(byte[] in, long off, long len) {
        if (len <= 32L) {
            if (len <= 16L) {
                return hashLen0To16(in, off, len);
            } else {
                return hashLen17To32(in, off, len);
            }
        } else if (len <= 64L) {
            return cityHashLen33To64(in, off, len);
        }

        long x = fetch64(in, off + len - 40L);
        long y = fetch64(in, off + len - 16L) + fetch64(in, off + len - 56L);
        long z = hashLen16(fetch64(in, off + len - 48L) + len,
                fetch64(in, off + len - 24L));

        long vFirst, vSecond, wFirst, wSecond;

        // This and following 3 blocks are produced by a single-click inline-function refactoring.
        // IntelliJ IDEA ftw
        // WeakHashLen32WithSeeds
        long a3 = len;
        long b3 = z;
        long w4 = fetch64(in, off + len - 64L);
        long x4 = fetch64(in, off + len - 64L + 8L);
        long y4 = fetch64(in, off + len - 64L + 16L);
        long z4 = fetch64(in, off + len - 64L + 24L);
        a3 += w4;
        b3 = rotateRight(b3 + a3 + z4, 21);
        long c3 = a3;
        a3 += x4 + y4;
        b3 += rotateRight(a3, 44);
        vFirst = a3 + z4;
        vSecond = b3 + c3;

        // WeakHashLen32WithSeeds
        long a2 = y + K1;
        long b2 = x;
        long w3 = fetch64(in, off + len - 32L);
        long x3 = fetch64(in, off + len - 32L + 8L);
        long y3 = fetch64(in, off + len - 32L + 16L);
        long z3 = fetch64(in, off + len - 32L + 24L);
        a2 += w3;
        b2 = rotateRight(b2 + a2 + z3, 21);
        long c2 = a2;
        a2 += x3 + y3;
        b2 += rotateRight(a2, 44);
        wFirst = a2 + z3;
        wSecond = b2 + c2;

        x = x * K1 + fetch64(in, off);

        len = (len - 1L) & (~63L);
        do {
            x = rotateRight(x + y + vFirst + fetch64(in, off + 8L), 37) * K1;
            y = rotateRight(y + vSecond + fetch64(in, off + 48L), 42) * K1;
            x ^= wSecond;
            y += vFirst + fetch64(in, off + 40L);
            z = rotateRight(z + wFirst, 33) * K1;

            // WeakHashLen32WithSeeds
            long a1 = vSecond * K1;
            long b1 = x + wFirst;
            long w2 = fetch64(in, off);
            long x2 = fetch64(in, off + 8L);
            long y2 = fetch64(in, off + 16L);
            long z2 = fetch64(in, off + 24L);
            a1 += w2;
            b1 = rotateRight(b1 + a1 + z2, 21);
            long c1 = a1;
            a1 += x2 + y2;
            b1 += rotateRight(a1, 44);
            vFirst = a1 + z2;
            vSecond = b1 + c1;

            // WeakHashLen32WithSeeds
            long a = z + wSecond;
            long b = y + fetch64(in, off + 16L);
            long w1 = fetch64(in, off + 32L);
            long x1 = fetch64(in, off + 32L + 8L);
            long y1 = fetch64(in, off + 32L + 16L);
            long z1 = fetch64(in, off + 32L + 24L);
            a += w1;
            b = rotateRight(b + a + z1, 21);
            long c = a;
            a += x1 + y1;
            b += rotateRight(a, 44);
            wFirst = a + z1;
            wSecond = b + c;

            long tmp = x;
            x = z;
            z = tmp;

            len -= 64L;
            off += 64L;
        } while (len != 0);
        return hashLen16(hashLen16(vFirst, wFirst) + shiftMix(y) * K1 + z,
                hashLen16(vSecond, wSecond) + x);
    }

    public static String evaluate(String str) {
        if (str == null)
            return "";
        long h = BigoIndigoUidHash.cityHash64(str.getBytes(), 0, str.length());
        return Long.toString(Math.abs(h));
    }
}