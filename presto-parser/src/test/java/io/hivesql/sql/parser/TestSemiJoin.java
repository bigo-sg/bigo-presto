package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 12/4/19 12:10 PM
 */
public class TestSemiJoin extends SQLTester {

    @Test
    public void testSemiJoin() {
        checkASTNodeFromFile("hive/parser/cases/semi-join-presto.sql",
                "hive/parser/cases/semi-join-hive.sql");
    }

    @Test
    public void testSemiJoin1() {
        checkASTNodeFromFile("hive/parser/cases/semi-join-presto1.sql",
                "hive/parser/cases/semi-join-hive1.sql");
    }
}
