package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 8/15/19 2:12 PM
 */
public class TestDesc extends SQLTester {

    @Test
    public void test01() {
        String sql = "desc tablename";
        checkASTNode(sql);
    }
    @Test
    public void test02() {
        String hiveSql = "desc a.tablename";
        String prestoSql = "desc a.tablename";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void test03() {
        String hiveSql = "desc table a.tablename";
        String prestoSql = "desc a.tablename";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void test06() {
        String hiveSql = "show columns from a.tablename";
        checkASTNode(hiveSql);
    }

    @Test
    public void test07() {
        String hiveSql = "show columns in tablename";
        checkASTNode(hiveSql);
    }
}
