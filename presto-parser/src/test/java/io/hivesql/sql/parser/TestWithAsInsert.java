package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 12/5/19 11:24 AM
 */
public class TestWithAsInsert extends SQLTester {
    @Test
    public void test01() {
        checkASTNodeFromFile("hive/parser/cases/with-as-insert-presto.sql",
                "hive/parser/cases/with-as-insert-hive.sql");
    }
    @Test
    public void test02() {
        checkASTNodeFromFile("hive/parser/cases/with-as-insert-presto1.sql",
                "hive/parser/cases/with-as-insert-hive1.sql");
    }
    @Test
    public void test03() {
        checkASTNodeFromFile("hive/parser/cases/with-as-insert-presto2.sql",
                "hive/parser/cases/with-as-insert-hive2.sql");
    }
}
