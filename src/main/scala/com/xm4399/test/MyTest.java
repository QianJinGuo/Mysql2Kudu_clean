package com.xm4399.test;

import com.xm4399.util.CreateKuduTable;
import com.xm4399.util.GetTableStru;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyTest {


    /*public static void main(String[] args) {
        GetTableStru getTableStru = new GetTableStru();
        CreateKuduTable createKuduTable = new CreateKuduTable();
        //createKuduTable.createKuduTable(getTableStru.getTableStru("chenzhikun_test","chenzhikun_test_3"),"chenzhikun_test_3");
        createKuduTable.createKuduTable(getTableStru.getTableStru(args[0],args[1]),args[1]);

    }*/
    public static void main(String[] args) {
        //String rex = "[unsignes]";
        String str = "varchar(100)";
        str = "text";
        str.trim();
        //macthTest(rex,str);


        String[] arr = str.split("\\(");
        if (1 == arr.length) {
            System.out.println(arr[0]);
        }
        if (2 == arr.length) {
            System.out.println(arr[0]);
            String str2 = arr[1];
            if (str2.trim().endsWith("unsigned")) {

            }
        }


    }





    public static void macthTest(String rex, String str) {

        Pattern p = Pattern.compile(rex); //编译对象
        Matcher m = p.matcher(str); //进行匹配
        while (m.find()) {
            System.out.println(m.group()); //默认是group(0)

        }



    }


}