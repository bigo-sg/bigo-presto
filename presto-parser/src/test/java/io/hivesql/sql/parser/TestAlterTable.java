package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 10/14/19 2:28 PM
 */
public class TestAlterTable extends SQLTester {
    @Test
    public void test01() {
        String prestoSql = "alter table tbl add column \"col\" BIGINT comment 'c'";
        String hiveSql = "alter table tbl add columns (col BIGINT comment 'c')";
        checkASTNode(prestoSql, hiveSql);
    }
    @Test
    public void test02() {
        String prestoSql = "alter table tbl add column \"col\" ARRAY(ROW(S BIGINT)) comment 'c'";
        String hiveSql = "alter table tbl add columns (col ARRAY<STRUCT<S:BIGINT>> comment 'c')";
        checkASTNode(prestoSql, hiveSql);
    }
    @Test
    public void test03() {
        String prestoSql = "alter table tbl rename to tbl1";
        checkASTNode(prestoSql);
    }
}
