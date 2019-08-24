package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class TestShowFunctions extends SQLTester {

    @Test
    public void testShowFunctions()
    {
        String hiveSql = "SHOW FUNCTIONS";
        checkASTNode(hiveSql);
    }
}
