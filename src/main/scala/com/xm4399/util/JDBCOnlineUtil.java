package com.xm4399.util;

import org.apache.commons.math3.analysis.polynomials.PolynomialFunctionNewtonForm;

import java.sql.*;
import java.util.*;


/**
 * @Auther: czk
 * @Date: 2020/8/26
 * @Description:
 */
public class JDBCOnlineUtil {

    // 获取mysql全表的结构信息
    public LinkedHashMap <String,String> getTablePriKeyStru(String address, String username, String password, String dbName, String tableName, String fields){
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        LinkedHashMap <String,String>  fieldAndIsPKMap = new LinkedHashMap <String,String>();
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
                fieldAndIsPKMap.put(name,isPriKey);
            }
            //如果按字段拉取的情况,map删除不必要的字段
            if (!"false".equals(fields)){
                List<String> fieldsList = Arrays.asList(fields.split(","));
                Iterator<Map.Entry<String, String>> it = fieldAndIsPKMap.entrySet().iterator();
                while (it.hasNext()){
                    Map.Entry<String,String> entry = it.next();
                    String field = entry.getKey();
                    if (!fieldsList.contains(field)){
                        it.remove();
                    }
                }
            }
            if (fieldAndIsPKMap.size() ==0 ){
                System.out.println("获取表 " + tableName + " 结构的Map为空" );
            }
            return fieldAndIsPKMap;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(res,stmt,con);
        }
        return null;
    }

  /*  // 按字段拉取的情况下,获取mysql相应字段信息
    public  LinkedHashMap<String, String> getTableStruFields(String address, String username, String password, String dbName, String tableName, String fields){
        Connection con = null; ;
        Statement stmt = null;
        ResultSet res = null;
        LinkedHashMap<String,String>  fieldInfoMap = new LinkedHashMap<String, String>();
        try {
            con = getConnection(address, username, password, dbName);
            stmt = con.createStatement();
            // column_key表示是否为主键,是的话返回"PRI",否的话返回空字符串
            String sql = "select column_name,column_key from  information_schema.columns " +
                    "where table_schema = " + "\""  + dbName + "\""  +"  and table_name  = " + "\""  +tableName +  "\"" +";"  ;
            res = stmt.executeQuery(sql);
            List<String> fieldsList = null;
            String[] fieldsArr = fields.split(",");
            fieldsList = Arrays.asList(fieldsArr);
            while (res.next()) {
                String name = res.getString(1);
                String isKey = res.getString(2);
                if (fieldsList.contains(name)){
                    fieldInfoMap.put(name, isKey);
                }
            }
            if (fieldInfoMap.size() ==0 ){
                System.out.println("该表 " + tableName + " 不存在" );
            }
            return fieldInfoMap;
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            close(res, stmt, con);
        }
        return null;
    }*/

    // 获取mysql表所有字段名的list集合
    public LinkedList <String> listAllFields(String address, String username, String password, String dbName, String tableName){
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        LinkedList<String> fieldsList = new LinkedList<String>();
        try {
            con = getConnection(address, username, password, dbName);
            stmt = con.createStatement();
            // column_key表示是否为主键,是的话返回"PRI",否的话返回空字符串
            String sql = "select column_name  from information_schema.columns " +
                    "where table_schema = " + "\""  + dbName + "\""  +"  and table_name  = " + "\""  +tableName +  "\"" +";"  ;
            res = stmt.executeQuery(sql);
            while (res.next()) {
               String field = res.getString(1);
               fieldsList.add(field);
            }

            return fieldsList;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(res,stmt,con);
        }
        return null;
    }

    // 获取mysql表字段类型为mediumtext,longtext的字段名集合.kudu单元格最大存64k
    public LinkedList <String> listLongTextFields(String address, String username, String password, String dbName, String tableName){
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        LinkedList<String> longTextFieldsList = new LinkedList<String>();
        try {
            con = getConnection(address, username, password, dbName);
            stmt = con.createStatement();
            // column_key表示是否为主键,是的话返回"PRI",否的话返回空字符串
            String sql = "select column_name , DATA_TYPE from information_schema.columns " +
                    "where table_schema = " + "\""  + dbName + "\""  +"  and table_name  = " + "\""  +tableName +  "\"" +";"  ;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                String dataType = res.getString(2);
                if ("mediumtext".equals(dataType) || "longtext".equals(dataType)){
                    String field = res.getString(1);
                    longTextFieldsList.add(field);
                }

            }

            return longTextFieldsList;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(res,stmt,con);
        }
        return null;
    }


    // 获取所有分表名
    public  ArrayList<String> listAllSubTableName(String address, String username, String password, String  dbName, String tableName){
        ArrayList<String> allSubTableList = new ArrayList<String>() ;
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            con = getConnection(address,username,password,dbName);
            stmt = con.createStatement();
            //获取以tableName_开头的 后面跟着0-100  的所有表的表名
            String sql = "select table_name from information_schema.tables where table_name REGEXP '"  + tableName + "_" + "[0-9]{1,3}';";
            res = stmt.executeQuery(sql);
            while (res.next()) {
                String subTableName = res.getString(1);
                allSubTableList.add(subTableName);
            }
            return allSubTableList;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           close(res,stmt,con);
        }
        return allSubTableList;
    }

    // 获取mysql表的记录数
    public  Long getTableCount(String address, String username, String password, String dbName, String tableName,
                              String timestampField, String timestampStr) throws SQLException {
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        LinkedHashMap <String,String>  fieldAndIsPKMap = new LinkedHashMap <String,String>();
        try {
            con = getConnection(address, username, password, dbName);
            stmt = con.createStatement();
            //获取指定时间戳前的记录数
            String sql = "select count(1) from " + tableName + " where " + timestampField + " <= " + timestampStr  ;
            res = stmt.executeQuery(sql);
            res.next();
            Long count = res.getLong(1);
            System.out.println("记录数为 >>>>" + count);
            return count;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            res.close();
            stmt.close();
            con.close();
        }
        return -1L;
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
