package com.xm4399.test;

import java.io.*;

public class MyTest {


    public static void main(String[] args) throws IOException {
        try {
            String[] arr = new String[1];
            arr[0]= "aa";
            arr[1] = "bb";
        } catch(Exception e ){
            System.out.println(">>>>>>>>>>>" + getException(e));;
        }

    }


    /**
     * 将异常日志转换为字符串
     * @param e
     * @return
     */
    public static String getException(Exception e) {
        Writer writer = null;
        PrintWriter printWriter = null;
        try {
            writer = new StringWriter();
            printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            return writer.toString();
        } finally {
            try {
                if (writer != null)
                    writer.close();
                if (printWriter != null)
                    printWriter.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }








}