package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Node;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

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

    private void checkASTNode(String prestoSql, String hiveSql) {
        Node prestoNode = usePrestoParser(prestoSql);
        System.out.println(prestoNode);

        Node hiveNode = useHiveParser(hiveSql);
        System.out.println(hiveNode);

        Assert.assertEquals(hiveNode, prestoNode);
    }

    private void checkASTNode(String sql) {
        checkASTNode(sql, sql);
    }

    @BeforeTest
    public void init() {
        sqlParser = new SqlParser();

        hiveParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
        hiveParsingOptions.setUseHiveSql(true);

        prestoParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
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
    public void testSelectIntegerLiteral()
    {
        String sql = "SELECT 1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectDoubleLiteral()
    {
        String sql = "SELECT 1.5";

        checkASTNode(sql);
    }

    @Test
    public void testSelectStringLiteral()
    {
        String sql = "SELECT 'abc'";

        checkASTNode(sql);
    }

    @Test
    public void testSelectStringLiteralWithDoubleQuotation()
    {
        String hiveSql = "SELECT \"abc\"";
        String prestoSql = "SELECT 'abc'";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testSelect02()
    {
        String sql = "SELECT a,b,c,d FROM tb1";

        checkASTNode(sql);
    }
}
