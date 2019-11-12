package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestUnBase64 {
    @Test
    private void testUnBase64() {
        Slice slice1 = UnBase64.unBase64Varchar(null);
        Slice slice2 = UnBase64.unBase64Varbinary(null);
        
        assertNull(slice1);
        assertNull(slice2);
    }
}
