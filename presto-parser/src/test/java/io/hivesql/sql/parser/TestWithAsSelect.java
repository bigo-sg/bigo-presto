package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class TestWithAsSelect extends SQLTester {

    @Test
    public void test01() {
        checkASTNodeFromFile("hive/parser/cases/with-as-select.sql");
    }
}
