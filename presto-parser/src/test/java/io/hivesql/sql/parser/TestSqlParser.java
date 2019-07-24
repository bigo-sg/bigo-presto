package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.tree.Node;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.parser.IdentifierSymbol.COLON;

public class TestSqlParser {

    private SqlParser sqlParser = null;
    private ParsingOptions hiveParsingOptions = null;
    private ParsingOptions prestoParsingOptions = null;

    private Node useHiveParser(String sql) {
        return sqlParser.createStatement(sql, hiveParsingOptions);
    }
    private Node usePrestoParser(String sql) {
        return sqlParser.createStatement(sql, prestoParsingOptions);
    }

    private void checkASTNode(String sql) {
        Node prestoNode = usePrestoParser(sql);
        System.out.println(prestoNode);

        Node hiveNode = useHiveParser(sql);
        System.out.println(hiveNode);

        Assert.assertEquals(hiveNode, prestoNode, "Unmatch SQL: " + sql);
    }

    @BeforeTest
    public void init() {
        sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON));

        hiveParsingOptions = new ParsingOptions();
        hiveParsingOptions.setUseHiveSql(true);

        prestoParsingOptions = new ParsingOptions();
        prestoParsingOptions.setUseHiveSql(false);
    }

//    @Test
//    public void testUse()
//    {
//        String sql = "USE hive.tmp";
//        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
//        System.out.println(node);
//    }
//
//    @Test
//    public void testSetSession()
//    {
//        String sql = "SET SESSION foo=true";
//        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
//        System.out.println(node);
//    }
//
//    @Test
//    public void testLogicalBinary()
//    {
//        String sql = "SELECT x FROM t";
//        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
//        System.out.println(node);
//    }

    @Test
    public void testSelect01()
    {
        String sql = "SELECT 1";

        checkASTNode(sql);
    }

    @Test
    public void testSelect02()
    {
        String sql = "SELECT a,b,c,d FROM tb1";

        checkASTNode(sql);
    }
}
