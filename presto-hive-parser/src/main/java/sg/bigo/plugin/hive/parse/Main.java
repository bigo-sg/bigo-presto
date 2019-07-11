package sg.bigo.plugin.hive.parse;

public class Main {
    public static void main(String[] args) throws ParseException {
        ParseDriver driver = new ParseDriver();

        String sql = "select c, count(1) as cnt from tb1 where a.f1 > 10 and a.f2 <= 'aabbcc' group by c";
        ASTNode node = driver.parse(sql);

        System.out.println(findRootNonNullToken(node));
    }

    private static ASTNode findRootNonNullToken(ASTNode tree) {
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        return tree;
    }

}
