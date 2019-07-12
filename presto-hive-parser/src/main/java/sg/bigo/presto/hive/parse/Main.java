package sg.bigo.presto.hive.parse;

public class Main {
    public static void main(String[] args) throws HiveParseException {
        HiveParseDriver driver = new HiveParseDriver();

        String sql = "select c, count(1) as cnt from tb1 where a.f1 > 10 and a.f2 <= 'aabbcc' group by c";
        ASTNode node = driver.parse(sql);

        System.out.println(node);

        try {
            String sqlWithError = "select from tb1 where a.f1 > 10 and a.f2 <= 'aabbcc' group by c";
            driver.parse(sqlWithError);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static ASTNode findRootNonNullToken(ASTNode tree) {
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        return tree;
    }

}
