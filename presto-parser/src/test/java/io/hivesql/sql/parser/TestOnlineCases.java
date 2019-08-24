package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 8/13/19 4:36 PM
 */
public class TestOnlineCases extends SQLTester {
    @Test
    public void test01() {
        checkASTNodeFromFile("hive/parser/cases/online-case-01.sql");
    }

    @Test
    public void test02() {
        checkASTNodeFromFile("hive/parser/cases/from-multi-table-presto-01.sql",
                "hive/parser/cases/from-multi-table-hive-01.sql");
    }

    @Test
    public void test03() {
        checkASTNodeFromFile("hive/parser/cases/online-case-04.sql");
    }
}
