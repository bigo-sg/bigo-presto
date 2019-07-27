package io.prestosql.sql.hive;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.tree.Node;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.parser.IdentifierSymbol.COLON;

public class TestHiveSqlParser {

    private SqlParser sqlParser = null;
    private ParsingOptions hiveParsingOptions = null;
    private ParsingOptions prestoParsingOptions = null;
    @BeforeTest
    public void init() {
        sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON));
        hiveParsingOptions = new ParsingOptions();
        hiveParsingOptions.setIfUseHiveParser(true);
        prestoParsingOptions = new ParsingOptions();
        prestoParsingOptions.setIfUseHiveParser(false);
    }

    @Test
    public void testUse()
    {
        String sql = "USE hive.tmp";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testSetSession()
    {
        String sql = "SET SESSION foo=true";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testLogicalBinary()
    {
        String sql = "SELECT x FROM t";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testSelect01()
    {
        String sql = "SELECT \"a\",b,c,d FROM ALGO.t WHERE x=321 LIMIT 100";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testQuotedQuery()
    {
        String sql = "SELECT `a`,b,c,d FROM ALGO.t WHERE x=321 LIMIT 100";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }
    @Test
    public void testDoubleEq()
    {
        String sql = "SELECT `a`,b,c,d FROM ALGO.t WHERE x==321 LIMIT 100";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }
    @Test
    public void testTableStartWithDigit()
    {
        String sql = "select * from TMP.20171014_tmpdata limit 10";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testSplit()
    {
        String sql = "\"split\"(\"registertime\", ' ')[BIGINT '1']";
        Node node = sqlParser.createExpression(sql);
        System.out.println(node);
    }

    @Test
    public void testBinnary()
    {
        String sql = "select 111|112 as x from bigolive.presto_job_audit where day='2019-07-26' limit 1";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testUnnestWithOrdinality()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) WITH ORDINALITY AS event (pos, event)";

        Node node = sqlParser.createStatement(prestoSql, prestoParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testLateralViewWithOrdinality()
    {
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view posexplode(events) t1 as pos, event";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testUnnestMultiColumn()
    {
        String prestoSql = "" +
                "SELECT event.*, event1.*" +
                "FROM tb1 " +
                "CROSS JOIN UNNEST(events1) WITH ORDINALITY AS event1 (c1) " +
                "CROSS JOIN UNNEST(events) WITH ORDINALITY AS event (c)";

        Node node = sqlParser.createStatement(prestoSql, prestoParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testLateralViewMultiColumn()
    {
        String hiveSql = "" +
                "SELECT event.*, event1.*" +
                "FROM tb1 " +
                "lateral view explode(events) event as c " +
                "lateral view explode(events1) event1 as c1";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testLateralViewMultiColumn1()
    {
        String hiveSql = "SELECT numbers, animals,c,c2\n" +
                "FROM (\n" +
                "  VALUES\n" +
                "    (ARRAY[2, 5], ARRAY['dog', 'cat', 'bird']),\n" +
                "    (ARRAY[7, 8, 9], ARRAY['cow', 'pig'])\n" +
                ") AS x (numbers, animals)\n" +
                "lateral view explode(numbers) t as c\n" +
                " lateral view explode(animals) t1 as c1";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }


}