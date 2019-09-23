package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestConcatWs {
    @Test
    public void testConcatWs() {
        assertEquals(ConcatWsFunction.concat_ws(utf8Slice("#"), utf8Slice("A")), utf8Slice("A"));
        assertEquals(ConcatWsFunction.concat_ws(utf8Slice("#"), null, utf8Slice("A")), utf8Slice("A"));
        assertEquals(ConcatWsFunction.concat_ws(utf8Slice("#"), utf8Slice("A"), utf8Slice("B")), utf8Slice("A#B"));
        assertEquals(ConcatWsFunction.concat_ws(utf8Slice("#"), utf8Slice("A"),null, utf8Slice("B")), utf8Slice("A#B"));
        assertEquals(ConcatWsFunction.concat_ws(utf8Slice("#"), utf8Slice("A"),null, null), utf8Slice("A"));
    }
}
