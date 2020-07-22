package com.xm4399.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;


public class GetAllSmallTableName {
    //获取所有分表名
    public static ArrayList<String> getAllSmallTableName(String  dbName, String tableName){
        ArrayList<String> allSmallTableList = new ArrayList<String>() ;
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql://10.0.0.92:3310/" + dbName,
                    "cnbbsReadonly", "LLKFN*k241235");

            Statement stmt = con.createStatement();
            //获取以tableName_开头的 后面跟着0-100  的所有表的表名
            String sql = "select table_name from information_schema.tables where table_name REGEXP '"  + tableName + "_" + "[0-9]{1,3}';";
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
                String smallTableName = res.getString(1);
                System.out.println("将表  " + smallTableName +"  加入集合>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                allSmallTableList.add(smallTableName);

            }
            res.close();
            stmt.close();
            con.close();
            return allSmallTableList;
        } catch (Exception e) {
            System.out.println(e);
        }
        return allSmallTableList;
    }

    /*public static void main(String[] args) {
        ArrayList<String> allSmallTableList = getAllSmallTableName(args[0],args[1]);
        for(String samllTableName : allSmallTableList){
            System.out.println(samllTableName);
        }
    }*/



    }

