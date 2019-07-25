package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ParseErrorMessages extends SQLTester {

    @Test
    public void antlr4ParseErrorMessage()
    {
        String sql = "SELECT a from b from c";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().startsWith("line 1:17: mismatched input 'from'. "));
        }
    }

    @Test
    public void sortByShouldThrowException2()
    {
        String sql = "SELECT a from b sort by c";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().startsWith("line 1:17: Don't support sort by"));
        }
    }
}
