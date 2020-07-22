package com.xm4399.test;

import org.apache.kudu.Hash;

import javax.xml.transform.Source;
import java.sql.*;
import java.util.*;


public class JdbcTest {

    //
    //根据数据库和表名,获取相应字段和属性

    public static LinkedHashMap<String, String[]> getDBTableStru()  {//(String dbName, String tableName){  //HashMap<String,Object>
        Connection con = null;
        LinkedHashMap<String,String[]>  fieldInfoMap = new LinkedHashMap<String, String[]>();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql://10.0.0.211:3307/chenzhikun_test",
                    "gprp", "gprp@@4399");

            Statement stmt = con.createStatement();
            //查询xx表各个字段的 类型, 是否为主键,(是主键返回"PRI",不是返回返回空字符串) ,是否可为空
            String sql = "select column_name,data_type,is_nullable,column_key from  information_schema.columns " +
                    "where table_schema = \"chenzhikun_test\" and table_name =\"chenzhikun_test_2\"; ";
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
                //System.out.println(list.size());
                /*System.out.println(fieldInfoArr.length);
                System.out.println(fieldInfoArr[2]);*/

                fieldInfoMap.put(name, fieldInfoArr);


            }

            res.close();
            stmt.close();
            con.close();
            return fieldInfoMap;
        } catch (Exception e) {
            System.out.println(e);
        }
        return fieldInfoMap;
    }

   /* public static void main(String[] args) throws SQLException {
        getDBTableStru();
        *//*ArrayList<String> dbTableNameList =getDbTableNameList();
        for(String s  : dbTableNameList){
            System.out.println(s);
        }
        selectAllRecord4EveryTable( dbTableNameList);

    }

    public static ArrayList<String>  getDbTableNameList(){
        ArrayList<String> dbTableNameList = new ArrayList<String>();
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306",
                    "canal", "canal");
            con.setReadOnly(true);
            Statement stmt = con.createStatement();
            String sql = "select table_schema,table_name from information_schema.tables " +
                    "where table_schema not in (\"information_schema\",\"mysql\",\"performance_schema\",\"sys\")";
            //String sql = "select * from chenzhikun.first_canal;";
            ResultSet res = stmt.executeQuery(sql);

            while (res.next()) {
                String stuff1 = res.getString(1);
                //res.
                //int  stuff1 = res.getInt(1);
                String stuff2 = res.getString(2);
                *//**//*String selectAllSql = "select * from " + stuff1 + "." + stuff2 + ";" ;
                ResultSet AllRecord = stmt.executeQuery(selectAllSql);*//**//*
                String dbTableName = stuff1 + "." + stuff2;
                dbTableNameList.add(dbTableName);
                *//**//*System.out.println( stuff1 +"    " + stuff2);
                System.out.println("------------------");*//**//*
            }

            res.close();
            stmt.close();
            con.close();
            return dbTableNameList;
        } catch (Exception e) {
            System.out.println(e);
        }
        return dbTableNameList;*//*

    }

    public static void selectAllRecord4EveryTable(ArrayList<String> dbTableNameList) throws SQLException {
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306",
                "canal", "canal");
        Statement stmt = con.createStatement();
        String sql = null;
        for(String dbTableName : dbTableNameList){
            sql = "select * from " + dbTableName +"  ;";
            //stmt.addBatch(sql);
            ResultSet res = stmt.executeQuery(sql);

            while (res.next()) {
                int  stuff1 = res.getInt(1);
                String stuff2 = res.getString(2);
                *//*String selectAllSql = "select * from " + stuff1 + "." + stuff2 + ";" ;
                ResultSet AllRecord = stmt.executeQuery(selectAllSql);*//*
                System.out.println( stuff1 +"    " + stuff2);
                System.out.println("------------------");
            }
            res.close();


        }

        stmt.close();
        con.close();
    }
*/

}
