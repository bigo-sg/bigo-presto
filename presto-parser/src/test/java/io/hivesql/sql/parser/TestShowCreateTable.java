package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class TestShowCreateTable extends SQLTester {

    @Test
    public void showCreateTable() {

        checkASTNodeFromFile("hive/parser/cases/show-create-table.sql");
    }
}
