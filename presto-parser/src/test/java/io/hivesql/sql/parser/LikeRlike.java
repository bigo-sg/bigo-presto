package io.hivesql.sql.parser;

import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;
import org.testng.annotations.Test;

import java.util.Optional;

public class LikeRlike extends SQLTester {

    @Test
    public void testRlike1()
    {
        String hiveSql = "select c from t where x RLIKE 'sdsada'";
        String prestoSql = "select c from t where regexp_like(cast(x as string), cast('sdsada' as string))";
        checkASTNode(prestoSql, hiveSql);
    }
}
