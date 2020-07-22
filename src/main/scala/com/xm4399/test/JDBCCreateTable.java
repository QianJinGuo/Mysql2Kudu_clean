package com.xm4399.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;

public class JDBCCreateTable {
    public static void main(String[] args) {
        Connection con = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/chenzhikun",
                    "canal", "canal");

            Statement stmt = con.createStatement();
            Statement stmt2 = con.createStatement();
            //查询xx表各个字段的 类型, 是否为主键,(是主键返回"PRI",不是返回返回空字符串) ,是否可为空
            String sql = "select column_name,data_type,is_nullable,column_key from  information_schema.columns " +
                    "where table_schema = \"chenzhikun\" and table_name =\"first_canal\"; ";
            //String sql = "select * from chenzhikun.first_canal;";
            ResultSet res = stmt.executeQuery(sql);

            while (res.next()) {
                String name = res.getString(1);
                String type = res.getString(2);

                String is_nullable = res.getString(3);
                String isKey = res.getString(4);
                System.out.println(name +"---"  +type +"---"  +is_nullable +"---"  + isKey );

                String[] fieldInfoArr = new String[3];
                fieldInfoArr[0] = type;
                fieldInfoArr[1] = is_nullable;
                fieldInfoArr[2] = isKey;

            }
            String sql2 = "select table_schema,table_name from information_schema.tables where table_schema " +
                    "not in (\"information_schema\",\"mysql\",\"performance_schema\",\"sys\"); ";
            ResultSet res2 = stmt2.executeQuery(sql2);

            while(res2.next()){
                String tableName = res2.getString(1);
                System.out.println(tableName);
            }


            res2.close();
            stmt2.close();
            res.close();
            stmt.close();
            con.close();

        } catch (Exception e) {
            System.out.println(e);
        }

    }


}
