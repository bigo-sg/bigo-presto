package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 12/4/19 12:10 PM
 */
public class TestCreateView extends SQLTester {

    @Test
    public void testCreateView() {
        checkASTNodeFromFile("hive/parser/cases/create-view-presto.sql",
                "hive/parser/cases/create-view-hive.sql");
    }
}
