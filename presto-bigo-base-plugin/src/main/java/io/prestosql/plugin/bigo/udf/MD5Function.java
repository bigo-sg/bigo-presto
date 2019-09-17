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

@Description("compute md5 hash")
@ScalarFunction("md5")
public final class MD5Function {
    private MD5Function() {
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice md5(@SqlType("T") Slice slice) {
        if (slice == null) {
            return null;
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
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
    public static Slice md5_long(@SqlType("T") long value) {
        String str = Long.toString(value);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
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
    public static Slice md5_double(@SqlType("T") double value) {
        String str = Double.toString(value);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
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
