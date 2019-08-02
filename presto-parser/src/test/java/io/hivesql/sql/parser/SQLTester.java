package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Node;
import org.testng.Assert;

public abstract class SQLTester {
    private static SqlParser sqlParser = new SqlParser();
    private static ParsingOptions hiveParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private static ParsingOptions prestoParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    protected Node useHiveParser(String sql) {
        return sqlParser.createStatement(sql, hiveParsingOptions);
    }
    protected Node usePrestoParser(String sql) {
        return sqlParser.createStatement(sql, prestoParsingOptions);
    }

    public void checkASTNode(String prestoSql, String hiveSql) {
        Node prestoNode = usePrestoParser(prestoSql);
        System.out.println(prestoNode);

        Node hiveNode = useHiveParser(hiveSql);
        System.out.println(hiveNode);

        Assert.assertEquals(hiveNode, prestoNode);
    }

    public void checkASTNode(Node prestoNode, Node hiveNode) {
        System.out.println(prestoNode);
        System.out.println(hiveNode);

        Assert.assertEquals(hiveNode, prestoNode);
    }

    public void checkASTNode(String sql) {
        checkASTNode(sql, sql);
    }

    public void runHiveSQL(String hiveSql) {
        useHiveParser(hiveSql);
    }

    public void runPrestoSQL(String prestoSql) {
        usePrestoParser(prestoSql);
    }

    static {
        hiveParsingOptions.setIfUseHiveParser(true);
        prestoParsingOptions.setIfUseHiveParser(false);
    }

}
