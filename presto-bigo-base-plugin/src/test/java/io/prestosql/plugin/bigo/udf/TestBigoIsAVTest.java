package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.testng.Assert.assertFalse;

public class TestBigoIsAVTest {

    @Test
    private void testIsAvTest() {
        boolean res1 = BigoIsAVTest.isAVTestOn(123, "test".getBytes(), 456);
        assertFalse(res1);
    }

    @Test
    private void test() throws UnsupportedEncodingException {
        byte[] bytes = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getEncoder().encodeToString(bytes);
        byte[] decoded = Base64.getDecoder().decode(encoded);
        System.out.println(encoded);
        System.out.println(encoded.getBytes());
        System.out.println(decoded);
    }
}
