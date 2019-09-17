package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;

@Description("Calculates the SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512) (as of Hive 1.3.0)." +
        " The first argument is the string or binary to be hashed. The second argument indicates the desired " +
        "bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which is equivalent to 256)")
@ScalarFunction("sha2")
public final class Sha2Function {
    private Sha2Function() {
    }
    private static final List<Integer> bitLengthList = Arrays.asList(224, 256, 384, 512, 0);

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sha2(@SqlType("T") Slice slice, @SqlType(StandardTypes.INTEGER) long bitLength) {
        if (slice == null || !bitLengthList.contains((int) bitLength)) {
            return null;
        }
        if (bitLength == 0) {
            bitLength = 256;
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-" + bitLength);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        digest.reset();
        digest.update(slice.getBytes(), 0, slice.toStringUtf8().length());
        byte[] md5Bytes = digest.digest();
        String md5Hex = Hex.encodeHexString(md5Bytes);

        return utf8Slice(md5Hex);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sha2_long(@SqlType("T") long value, @SqlType(StandardTypes.INTEGER) long bitLength) {
        if (!bitLengthList.contains((int) bitLength)) {
            return null;
        }
        if (bitLength == 0) {
            bitLength = 256;
        }

        String str = Long.toString(value);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-" + bitLength);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        digest.reset();
        digest.update(str.getBytes(), 0, str.length());
        byte[] md5Bytes = digest.digest();
        String md5Hex = Hex.encodeHexString(md5Bytes);

        return utf8Slice(md5Hex);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sha2_double(@SqlType("T") double value, @SqlType(StandardTypes.INTEGER) long bitLength) {
        if (!bitLengthList.contains((int) bitLength)) {
            return null;
        }
        if (bitLength == 0) {
            bitLength = 256;
        }

        String str = Double.toString(value);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-" + bitLength);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        digest.reset();
        digest.update(str.getBytes(), 0, str.length());
        byte[] md5Bytes = digest.digest();
        String md5Hex = Hex.encodeHexString(md5Bytes);

        return utf8Slice(md5Hex);
    }
}
