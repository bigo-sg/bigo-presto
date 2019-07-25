package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class BasicSQLs extends SQLTester {
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
        String prestoSql = "SELECT 'abc'";
        String hiveSql = "SELECT \"abc\"";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testSelectFromWithDistinct()
    {
        String sql = "SELECT distinct a from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromWithBacktick()
    {
        String prestoSql = "SELECT a FROM tb1";
        String hiveSql = "SELECT `a` FROM tb1";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testWhereClause()
    {
        String sql = "SELECT a, b from tb1 where c > 10 and c <> 'hello' or (d = true and e is not null)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIsNotNull()
    {
        String sql = "SELECT a from tb1 where e is not null";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIsNull()
    {
        String sql = "SELECT a from tb1 where e is null";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseBetween()
    {
        String sql = "SELECT a from tb1 where e BETWEEN start1 and end2";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotBetween()
    {
        String sql = "SELECT a from tb1 where e NOT BETWEEN start1 and end2";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIn()
    {
        String sql = "SELECT a from tb1 where e in(1, 2)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotIn()
    {
        String sql = "SELECT a from tb1 where e not in(1, 2)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseLike()
    {
        String sql = "SELECT a from tb1 where e like 'a%'";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotLike()
    {
        String sql = "SELECT a from tb1 where e not like 'a%'";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseDistinct()
    {
        String sql = "SELECT a from tb1 where e is distinct from end_day";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotDistinct()
    {
        String sql = "SELECT a from tb1 where e is not distinct from end_day";

        checkASTNode(sql);
    }

    @Test
    public void testLimit()
    {
        String sql = "SELECT a from tb1 limit 10";

        checkASTNode(sql);
    }

    @Test
    public void testOrderBy()
    {
        String sql = "SELECT a from tb1 order by t desc";

        checkASTNode(sql);
    }

    @Test
    public void testGroupBy()
    {
        String sql = "SELECT b as cnt from tb1 group by b";

        checkASTNode(sql);
    }


    @Test
    public void testGroupBy2()
    {
        String sql = "SELECT b, count(1) as cnt from tb1 group by b";

        checkASTNode(sql);
    }


    @Test
    public void testHaving()
    {
        String sql = "SELECT a from tb1 group by a having a < 10";

        checkASTNode(sql);
    }
}
