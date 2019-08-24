package io.hivesql.sql.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestOnlineBadCases extends SQLTester {

    @Test
    public void testCase01() {
        checkASTNodeFromFile("hive/parser/cases/plan-timeout.sql");
    }

    @Test
    public void testCase02() {
        try {
            runHiveSQLFromFile("hive/parser/cases/online-case-1.sql");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "line 1:1: please check, how should we merge them? " +
                    "most possible reason is executing a syntax that hive not support");
        }
    }

    @Test
    public void testCase03() {
        checkASTNodeFromFile("hive/parser/cases/online-case-03.sql", "hive/parser/cases/online-case-02.sql");
    }

}
