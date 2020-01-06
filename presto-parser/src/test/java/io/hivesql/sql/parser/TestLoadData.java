package io.hivesql.sql.parser;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.parser.hive.LoadData;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;

/**
 * @author tangyun@bigo.sg
 * @date 1/6/20 4:07 PM
 */
public class TestLoadData extends SQLTester {

    @Test
    public void test01() {
        String sql = "load data inpath 'location' overwrite into table tbl";
        Node sqlNode = runHiveSQL(sql);
        LoadData loadData = new LoadData(
                Optional.empty(),
                QualifiedName.of("tbl"),
                "'location'",
                true,
                ImmutableList.of()
        );
        Assert.assertEquals(sqlNode, loadData);
    }

    @Test
    public void test02() {
        String sql = "load data inpath 'location' into table tbl";
        Node sqlNode = runHiveSQL(sql);
        LoadData loadData = new LoadData(
                Optional.empty(),
                QualifiedName.of("tbl"),
                "'location'",
                false,
                ImmutableList.of()
        );
        Assert.assertEquals(sqlNode, loadData);
    }

    @Test
    public void test03() {
        String sql = "load data inpath 'location' into table tbl partition(a='a',b='b',c='c')";
        LoadData sqlNode = (LoadData)runHiveSQL(sql);
        Assert.assertEquals("/a=a/b=b/c=c/", sqlNode.getPartitionEnd());
    }
}