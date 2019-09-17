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

import static io.airlift.slice.Slices.utf8Slice;

@Description("calculates the sha-1 digest for string, and returns the value as a hex string.")
@ScalarFunction(value = "sha", alias = "sha1")
public final class ShaFunction {
    private ShaFunction() {
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sha(@SqlType("T") Slice slice) {
        if (slice == null) {
            return null;
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA");
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
    public static Slice sha_long(@SqlType("T") long value) {
        String str = Long.toString(value);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA");
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
    public static Slice sha_double(@SqlType("T") double value) {
        String str = Double.toString(value);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA");
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
