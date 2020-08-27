package com.xm4399.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * @Auther: czk
 * @Date: 2020/8/26
 * @Description:
 */
public class JDBCOnlineUtil {


    public ArrayList<LinkedHashMap <String,String>> getTableStru(String address, String username, String password, String dbName, String tableName,
                                                                 String isSubTable){
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        ArrayList<LinkedHashMap <String,String>>  fieldsInfoList = new ArrayList<LinkedHashMap <String,String>>();
        //如果传入的为分表,则从分表1获取表结构
        if("true".equals(isSubTable)){
            tableName = tableName + "_1";
        }
        try {
            con = getConnection(address, username, password, dbName);
            stmt = con.createStatement();
            // column_key表示是否为主键,是的话返回"PRI",否的话返回空字符串
            String sql = "select column_name , column_key from information_schema.columns " +
                    "where table_schema = " + "\""  + dbName + "\""  +"  and table_name  = " + "\""  +tableName +  "\"" +";"  ;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                String name = res.getString(1);
                String isPriKey = res.getString(2);
                LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();
                map.put(name,isPriKey);
                fieldsInfoList.add(map);
            }
            if (fieldsInfoList.size() ==0 ){
                System.out.println("该表 " + tableName + " 不存在" );
            }
            return fieldsInfoList;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(res,stmt,con);
        }
        return null;
    }

    public  Connection getConnection (String address, String username, String password,String dbName){
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + address + "/" + dbName, username, password);
            //connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/chenzhikun", "canal", "canal");
            return connection;
        } catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    public  void close(ResultSet res, Statement stmt, Connection con ) {
        try {
            if (res != null) {
                res.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            res = null;
            stmt = null;
            con = null;
        }
    }



}
