package com.xm4399.test;

import com.xm4399.util.ReaderUtil;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyTest {


    public static String getJarFile()throws IOException {
        InputStream in=MyTest.class.getClass().getResourceAsStream("/instance.properties");//读jar包根目录下的idcheck-file.properties文件
        //Reader f = new InputStreamReader(in);
        BufferedReader fb = new BufferedReader(new InputStreamReader(in));
        StringBuffer sb = new StringBuffer("");
        String s = "";
        while((s = fb.readLine()) != null) {
            sb = sb.append(s);
        }
        return sb.toString();
    }
    public static void main(String[] args) throws IOException {
     Long l = 1L;
        System.out.println(String.valueOf(l));
    }





    public static void macthTest(String rex, String str) {

        Pattern p = Pattern.compile(rex); //编译对象
        Matcher m = p.matcher(str); //进行匹配
        while (m.find()) {
            System.out.println(m.group()); //默认是group(0)

        }



    }


}