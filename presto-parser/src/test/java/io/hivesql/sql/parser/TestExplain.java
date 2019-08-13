package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 8/13/19 3:14 PM
 */
public class TestExplain extends SQLTester {

    @Test
    public void test01() {
        String sql = "explain select * from t";
        checkASTNode(sql);
    }
    @Test
    public void test02() {

        checkASTNodeFromFile("hive/parser/cases/explain-1.sql");
    }
}
