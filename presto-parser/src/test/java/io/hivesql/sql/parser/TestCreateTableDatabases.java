package io.hivesql.sql.parser;

import io.prestosql.sql.parser.hive.HiveAstBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCreateTableDatabases extends SQLTester {

    @Test
    public void testCreateDatabase() {

        String hiveSql = "" +
                "create database if not exists test with dbproperties" +
                "(" +
                "a=true," +
                "c=123," +
                "d=\"dsds\"" +
                ")";
        String prestoSql = "" +
                "create schema if not exists test with " +
                "(" +
                "a=true," +
                "c=123," +
                "d='dsds'" +
                ")";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testDropDatabase() {

        String hiveSql = "drop database if exists test";
        String prestoSql = "drop schema if exists test";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testCreateTable() {
        checkASTNodeFromFile("hive/parser/cases/create-table-presto-2.sql",
                "hive/parser/cases/create-table-hive-2.sql");
    }

    @Test
    public void createTableAsSelect() {

        checkASTNodeFromFile("hive/parser/cases/create-table-as-select-presto.sql",
                "hive/parser/cases/create-table-as-select-hive.sql");
    }

    @Test
    public void createTableAsSelect1() {

        checkASTNodeFromFile("hive/parser/cases/create-table-as-select-presto-1.sql",
                "hive/parser/cases/create-table-as-select-hive-1.sql");
    }

    @Test
    public void createTable2() {

        checkASTNodeFromFile("hive/parser/cases/create-table-presto-1.sql",
                "hive/parser/cases/create-table-hive-1.sql");
    }

    @Test
    public void transTypeSimple() {
        String input = "string";
        String expect = "VARCHAR";
        String result = HiveAstBuilder.colTypeTransformSimple(input);
        System.out.println(result);
        Assert.assertEquals(result, expect);
        input = "int";
        expect = "INTEGER";
        result = HiveAstBuilder.colTypeTransformSimple(input);
        Assert.assertEquals(result, expect);
        input = "bigint";
        expect = "BIGINT";
        result = HiveAstBuilder.colTypeTransformSimple(input);
        Assert.assertEquals(result, expect);
        input = "float";
        expect = "REAL";
        result = HiveAstBuilder.colTypeTransformSimple(input);
        Assert.assertEquals(result, expect);
    }

    @Test
    public void testTypeComplex() {
        String input = "array< struct <idx:bigint,extra:map<string,string>>, x:float, `table`:string>";
        String expect = "ARRAY(ROW(IDX BIGINT,EXTRA MAP(VARCHAR,VARCHAR)), X REAL, \"TABLE\" VARCHAR)";
        String result = HiveAstBuilder.colTypeTransformComplex(input);
        System.out.println(result);
        Assert.assertEquals(result, expect);
    }

    @Test
    public void createTable3() {

        checkASTNodeFromFile("hive/parser/cases/create-table-presto-3.sql",
                "hive/parser/cases/create-table-hive-3.sql");
    }

    @Test
    public void createTable4() {

        runHiveSQLFromFile("hive/parser/cases/create-table-hive-4.sql");
    }

    @Test
    public void testDropTable() {
        String hiveSql = "drop table if exists test";
        checkASTNode(hiveSql);
    }

    @Test
    public void testDropView() {
        String hiveSql = "drop view if exists a.test";
        checkASTNode(hiveSql);
    }

}
