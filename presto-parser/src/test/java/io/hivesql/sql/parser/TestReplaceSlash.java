package io.hivesql.sql.parser;

import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.StringLiteral;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 12/24/19 2:52 PM
 */
public class TestReplaceSlash extends SQLTester {

    @Test
    public void testReplaceRlikeSlash()
    {
        checkString("hive/parser/cases/slash-test.sql");
    }

    @Test
    public void testReplaceFunctionParaSlash()
    {
        checkString("hive/parser/cases/slash-test-function.sql");
    }

    @Test
    public void testReplaceFunctionParaSlash1()
    {
        checkString("hive/parser/cases/slash-test-function1.sql");
    }

    public void checkString(String path) {
        String[] hiveSql = getResourceContent(path)
                .replace("\n", "")
                .split(";");
        Node query = runHiveSQL(hiveSql[0]);
        traversalAstTree(query, node -> {
            if (node instanceof StringLiteral) {
                Assert.assertEquals(node.toString(), hiveSql[1]);
            }
        });
    }
}
