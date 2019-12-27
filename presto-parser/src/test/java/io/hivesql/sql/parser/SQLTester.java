package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Node;
import io.utils.FileUtils;
import org.testng.Assert;

public abstract class SQLTester {
    private static SqlParser sqlParser = new SqlParser();
    private static ParsingOptions hiveParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private static ParsingOptions prestoParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    protected Node useHiveParser(String sql) {
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
        return node;
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

    public void checkTypeASTNode(String prestoSql, String hiveSql) {
        Node prestoNode = sqlParser.createType(prestoSql, prestoParsingOptions);
        System.out.println(prestoNode);

        Node hiveNode = sqlParser.createType(hiveSql, hiveParsingOptions);
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

    public Node runHiveSQL(String hiveSql) {
        return useHiveParser(hiveSql);
    }

    public Node runPrestoSQL(String prestoSql) {
        return usePrestoParser(prestoSql);
    }

    public void checkASTNodeFromFile(String prestoPath, String hivePath) {
        String prestoSql = getResourceContent(prestoPath);
        String hiveSql = getResourceContent(hivePath);
        checkASTNode(prestoSql, hiveSql);
    }

    public void checkASTNodeFromFile(String sqlPath) {
        String sql = getResourceContent(sqlPath);
        checkASTNode(sql, sql);
    }

    public Node runHiveSQLFromFile(String hiveSqlPath) {
        String hiveSql = getResourceContent(hiveSqlPath);
        return useHiveParser(hiveSql);
    }

    public Node runPrestoSQLFromFile(String prestoSqlPath) {
        String prestoSql = getResourceContent(prestoSqlPath);
        return usePrestoParser(prestoSql);
    }

    static {
        hiveParsingOptions.setIfUseHiveParser(true);
        prestoParsingOptions.setIfUseHiveParser(false);
    }

    public String getResourceContent(String path) {
        String fullPath =
                this.getClass().
                        getResource("../../../../").
                        getFile() + path;
        return new String(FileUtils.getFileAsBytes(fullPath));
    }

    public void traversalAstTree(Node node, NodeAction nodeAction) {
        nodeAction.action(node);
        for (Node child: node.getChildren()) {
            traversalAstTree(child, nodeAction);
        }
    }

    public interface NodeAction {
        void action(Node node);
    }
}
