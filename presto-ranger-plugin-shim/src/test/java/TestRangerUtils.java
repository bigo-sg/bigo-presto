import com.alibaba.fastjson.JSON;
import io.utils.FileUtils;
import org.apache.ranger.authorization.presto.authorizer.utils.PrestoAccessType;
import org.apache.ranger.authorization.presto.authorizer.utils.RangerUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tangyun@bigo.sg
 * @date 9/25/19 5:04 PM
 */
public class TestRangerUtils {

    /**
     * unit test policies model:
     * user:
     * unit_user1, 所属组：unit_group，
     * unit_user2，所属source：unit_source,在ranger中找不到
     * unit_user3, 所属组：unit_group1
     * dbs:
     * unit_db1
     * unit_db2
     * unit_db3
     * unit_db4
     * policies:
     * unit_user1 可以访问unit_db1所有表（t1,t2），select、update
     * unit_group 可以访问unit_db2的除了t1表的所有表（t2,t3），select、update
     * unit_source 只可以访问unit_db3的t1，select、drop
     * unit_group1 对除了unit_db1，unit_db2，unit_db3对有所有db的所有表有访问权限,，select、update,create,drop
      */
    @BeforeTest
    public void prepare() {
        RangerUtils.setHivePolicies(JSON.parseObject(getResourceContent("policies")));
    }

    @Test
    public void testUser1() {
        List<String> groups = new ArrayList<>();
        groups.add("unit_group");
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "default",
                        "unit_user1",
                        "unit_db1",
                        "t1",
                        PrestoAccessType.SELECT
                ), true);

        groups.add("unit_group");
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "default",
                        "unit_user1",
                        "unit_db1",
                        "t1",
                        PrestoAccessType.DROP
                ), false);
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "default",
                        "unit_user1",
                        "unit_db2",
                        "t1",
                        PrestoAccessType.SELECT
                ), false);
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "default",
                        "unit_user1",
                        "unit_db2",
                        "t2",
                        PrestoAccessType.SELECT
                ), true);
    }

    @Test
    public void testUser2() {
        List<String> groups = new ArrayList<>();
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "unit_source",
                        "unit_user2",
                        "unit_db3",
                        "t1",
                        PrestoAccessType.DROP
                ), true);
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "unit_source",
                        "unit_user2",
                        "unit_db3",
                        "t1",
                        PrestoAccessType.INSERT
                ), false);
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "unit_source",
                        "unit_user2",
                        "unit_db2",
                        "t1",
                        PrestoAccessType.DROP
                ), false);
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "unit_source",
                        "unit_user2",
                        "unit_db3",
                        "t2",
                        PrestoAccessType.DROP
                ), false);
    }

    @Test
    public void testUser3() {
        List<String> groups = new ArrayList<>();
        groups.add("unit_group1");
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "default",
                        "unit_user3",
                        "unit_db4",
                        "t1",
                        PrestoAccessType.CREATE
                ), true);
        Assert.assertEquals(RangerUtils
                .checkPermission(
                        groups,
                        "default",
                        "unit_user3",
                        "unit_db1",
                        "t1",
                        PrestoAccessType.CREATE
                ), false);
    }

    public String getResourceContent(String path) {
        String fullPath =
                this.getClass().
                        getResource("./").
                        getFile() + path;
        return new String(FileUtils.getFileAsBytes(fullPath));
    }

}
