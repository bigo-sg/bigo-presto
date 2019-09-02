package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class TestHiveAuditCase extends SQLTester {

    @Test
    public void test01() {
        checkASTNodeFromFile("hive/parser/cases/hive_audit/case02.sql",
                "hive/parser/cases/hive_audit/case01.sql");
    }

}
