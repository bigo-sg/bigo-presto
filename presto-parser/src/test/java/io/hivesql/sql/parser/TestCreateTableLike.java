package io.hivesql.sql.parser;

import io.prestosql.sql.parser.hive.CreateTableLike;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

/**
 * @author tangyun@bigo.sg
 * @date 12/19/19 6:19 PM
 */
public class TestCreateTableLike extends SQLTester {
    @Test
    public void test() {
        String sql = "create table if not exists a like b";
        Node node = runHiveSQL(sql);
        QualifiedName a = QualifiedName.of("a");
        QualifiedName b = QualifiedName.of("b");

        CreateTableLike createTableLike = new CreateTableLike(
                Optional.empty(),
                a,
                b,
                true
        );
        checkASTNode(node, createTableLike);
    }
}
