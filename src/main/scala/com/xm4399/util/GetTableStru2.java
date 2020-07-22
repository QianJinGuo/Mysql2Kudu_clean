package com.xm4399.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;

public class GetTableStru2 {

    //根据数据库和表名,获取相应字段和属性

    public static LinkedHashMap<String, String[]> getTableStru(String dbName, String tableName, int argsLen){
        Connection con = null;
        LinkedHashMap<String,String[]>  fieldInfoMap = new LinkedHashMap<String, String[]>();
        //如果传入的为分表,则从分表   获取表结构
        if( 3 == argsLen){
            tableName = tableName + "_1";
        }
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql://10.0.0.92:3310/" + dbName,
                    "cnbbsReadonly", "LLKFN*k241235");

            Statement stmt = con.createStatement();
            //查询xx表各个字段的 类型, 是否为主键,(是主键返回"PRI",不是返回返回空字符串) ,是否可为空
            String sql = "select column_name,COLUMN_TYPE,is_nullable,column_key from  information_schema.columns " +
                    "where table_schema = " + "\""  + dbName + "\""  +"  and table_name  = " + "\""  +tableName +  "\"" +";"  ;
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
                String name = res.getString(1);
                String type = res.getString(2);
                String is_nullable = res.getString(3);
                String isKey = res.getString(4);

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
}
