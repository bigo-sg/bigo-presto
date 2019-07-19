package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.tree.Node;
import org.testng.annotations.Test;

import static io.prestosql.sql.parser.IdentifierSymbol.COLON;

public class TestSqlParser {

    @Test
    public void testSetSession()
    {

        String sql = "select * from x";
        String sql1 = "SET SESSION foo=true";
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON));
        ParsingOptions parsingOptions = new ParsingOptions();
        parsingOptions.setUseHiveSql(true);
        Node node = sqlParser.createStatement(
                sql1, parsingOptions);
        System.out.println(node);
    }
}
