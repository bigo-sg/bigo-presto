package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class TestInsert extends SQLTester {

    @Test
    public void insertIntoSelect() {

        checkASTNodeFromFile("hive/parser/cases/insert-into-select-hive.sql");
    }
}
