package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class CreateTableDatabases extends SQLTester {

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
    public void createTableAsSelect2() {

        checkASTNodeFromFile("hive/parser/cases/create-table-presto-1.sql",
                "hive/parser/cases/create-table-hive-1.sql");
    }

}
