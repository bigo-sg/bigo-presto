package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class Joins extends SQLTester {

    @Test
    public void testJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testInnerJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testLeftOuterJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "LEFT OUTER JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testLeftJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "LEFT JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testRightOuterJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "RIGHT OUTER JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testRightJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "RIGHT JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testFullOuterJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "FULL Outer JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testFullJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "FULL JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testThreeJoins()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate, Items.Name\n" +
                "FROM Orders\n" +
                "JOIN Customers ON Orders.CustomerID=Customers.CustomerID\n" +
                "JOIN Items ON Orders.ItemID=Items.ID AND Orders.ItemType=Items.Type" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testJoinsAliasedRelation()
    {
        String sql = "" +
                "SELECT * " +
                "FROM\n" +
                "  (SELECT hdid\n" +
                "   FROM like_dw_com.dwd_like_com_dim_hdid_basic_info\n" +
                "   WHERE app_install_day='2019-11-21') t1\n" +
                "LEFT JOIN (like_dw_com.dwd_like_com_dim_uid_hdid_map) t3 ON t1.hdid=t3.hdid";

        checkASTNode(sql);
    }
}
