package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 11/12/19 5:39 PM
 */
public class TestTypes extends SQLTester {

    @Test
    public void testType1()
    {
        String hiveSql = "int";
        String prestoSql = "int";
        checkTypeASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testType2()
    {
        String hiveSql = "array<int>";
        String prestoSql = "array(int)";
        checkTypeASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testType3()
    {
        String hiveSql = "struct<c1:int>";
        String prestoSql = "row(c1 int)";
        checkTypeASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testType4()
    {
        String hiveSql = "map<int,int>";
        String prestoSql = "map(int,int)";
        checkTypeASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testType5()
    {
        String hiveSql = "array<struct<time:bigint,lng:bigint,lat:bigint,net:string,log_extra:map<string,string>,event_id:string,event_info:map<string,string>>>";
        String prestoSql = "array(row(time bigint, lng bigint, lat bigint, net string, log_extra map(string, string), event_id string, event_info map(string, string)))";
        checkTypeASTNode(prestoSql, hiveSql);
    }
}
