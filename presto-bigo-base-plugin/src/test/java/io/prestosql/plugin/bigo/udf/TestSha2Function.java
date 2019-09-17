package io.prestosql.plugin.bigo.udf;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import java.util.Objects;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestSha2Function
        extends AbstractTestFunctions
{
    @Test
    public void testSHA2()
    {
        assertEquals(Sha2Function.sha2(utf8Slice("ABC"), 224).toStringUtf8(), "107c5072b799c4771f328304cfe1ebb375eb6ea7f35a3aa753836fad");
        assertEquals(Sha2Function.sha2(utf8Slice("ABC"), 256).toStringUtf8(), "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78");
        assertEquals(Sha2Function.sha2(utf8Slice("ABC"), 384).toStringUtf8(), "1e02dc92a41db610c9bcdc9b5935d1fb9be5639116f6c67e97bc1a3ac649753baba7ba021c813e1fe20c0480213ad371");
        assertEquals(Sha2Function.sha2(utf8Slice("ABC"), 512).toStringUtf8(), "397118fdac8d83ad98813c50759c85b8c47565d8268bf10da483153b747a74743a58a90e85aa9f705ce6984ffc128db567489817e4092d050d8a1cc596ddc119");
        assertEquals(Sha2Function.sha2(utf8Slice("ABC"), 0).toStringUtf8(), "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78");

        assertEquals(Objects.requireNonNull(Sha2Function.sha2_long(111, 0)).toStringUtf8(), "f6e0a1e2ac41945a9aa7ff8a8aaa0cebc12a3bcc981a929ad5cf810a090e11ae");
        assertEquals(Objects.requireNonNull(Sha2Function.sha2_long(111, 256)).toStringUtf8(), "f6e0a1e2ac41945a9aa7ff8a8aaa0cebc12a3bcc981a929ad5cf810a090e11ae");

        assertNull(Sha2Function.sha2(utf8Slice("ABC"), 100));
    }
}
