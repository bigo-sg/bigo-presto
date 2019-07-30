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

        String hiveSql = "" +
                "create table if not exists tmp.test with dbproperties" +
                "(" +
                "a=true," +
                "c=123" +
                ")";
        String prestoSql = "" +
                "create table if not exists tmp.test with " +
                "(" +
                "a=true," +
                "c=123" +
                ")";
        checkASTNode(prestoSql, hiveSql);
    }

}
