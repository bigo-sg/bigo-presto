package sg.bigo.presto.hive.parse;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 7/15/19 6:01 PM
 */
public class HiveParseDriverTests {

    @Test
    public void test01() {
        HiveParseDriver driver = new HiveParseDriver();

        String sql = "select * from (select count(distinct uid) as dau from tab group by os)t0 limit 100";
        ASTNode node = null;
        try {
            node = driver.parse(sql);
        } catch (HiveParseException e) {
            e.printStackTrace();
        }

        System.out.println(node);
    }

    @Test
    public void test02() {
        HiveParseDriver driver = new HiveParseDriver();

        String sql = "show create table t";
        ASTNode node = null;
        try {
            node = driver.parse(sql);
        } catch (HiveParseException e) {
            e.printStackTrace();
        }

        System.out.println(node);
    }

}
