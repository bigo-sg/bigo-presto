package io.hivesql.sql.parser;

import io.prestosql.sql.parser.hive.SetHiveConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SetHiveConfigurations extends SQLTester {

    @Test
    public void test()
    {
        String sql = "set hive.merge.mapfiles=false";

        SetHiveConfiguration node = (SetHiveConfiguration) runHiveSQL(sql);

        Assert.assertEquals(node, new SetHiveConfiguration("sethive.merge.mapfiles=false"));
    }
}
