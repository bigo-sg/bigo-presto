package io.prestosql.operator.scalar;

import org.testng.annotations.Test;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestBigoConcat
        extends AbstractTestFunctions {

    @Test
    public void testConcat() {
        assertFunction("concat(1, '2')", VARCHAR, "12");
        assertFunction("concat(1, 'a', 'b', 2.0)", VARCHAR, "1ab2.0");
        assertFunction("concat(1, 2)", VARCHAR, "12");
        assertFunction("concat(1.2, 3)", VARCHAR, "1.23");
    }

    @Test
    public void testConcatWsArray() {
        assertFunction("concat_ws('#', array['a'])", VARCHAR, "a");
        assertFunction("concat_ws('#', array['a', null])", VARCHAR, "a#null");
        assertFunction("concat_ws('#', array['a', ''])", VARCHAR, "a#");
        assertFunction("concat_ws('#', array ['a', 'b', 'c'])", VARCHAR, "a#b#c");
        assertFunction("concat_ws('#', array ['a', null, 'b', ''])", VARCHAR, "a#null#b#");
        assertFunction("concat_ws('#', array['a', 'b', 'c'])", VARCHAR, "a#b#c");
    }

//    @Test
//    public void testConcatWs() {
//        assertFunction("concat_ws('#', 'a', 'b')", VARCHAR, "a#b");
//    }
}
