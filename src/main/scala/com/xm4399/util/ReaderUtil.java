package com.xm4399.util;

import com.xm4399.test.MyTest;

import java.io.*;
import java.util.Properties;

/**
 * @Auther: czk
 * @Date: 2020/7/30
 * @Description:
 */
public class ReaderUtil {
    public static String getInstanceConfString (String address, String userName, String password, String dbName,
                                         String tableName, String topic){

        String filterRegex = dbName + "\\\\..*";
        StringBuilder stringBuilder = null;
        BufferedReader br = null;
        String line = null;
        try {
            stringBuilder = new StringBuilder();
            InputStream in= ReaderUtil.class.getClass().getResourceAsStream("/instance.properties");
            //InputStream in= ReaderUtil.class.getClass().getResourceAsStream("src/main/resources/instance.properties");
            br = new BufferedReader(new InputStreamReader(in));
            while ((line = br.readLine()) != null){
                if (line.startsWith("canal.instance.master.address=")){
                    stringBuilder.append("canal.instance.master.address=" + address + "\n");
                }else if (line.startsWith("canal.instance.dbUsername=")){
                    stringBuilder.append("canal.instance.dbUsername=" + userName + "\n");
                }else if (line.startsWith("canal.instance.dbPassword=")){
                    stringBuilder.append("canal.instance.dbPassword=" + password + "\n");
                }else if (line.startsWith("canal.instance.defaultDatabaseName=")){
                    stringBuilder.append("canal.instance.defaultDatabaseName=" + dbName + "\n");
                }else if (line.startsWith("canal.instance.filter.regex=")){
                    stringBuilder.append("canal.instance.filter.regex=" + filterRegex + "\n");
                } else if (line.startsWith("canal.mq.topic=")){
                    stringBuilder.append("canal.mq.topic=" + topic + "\n");
                }else {
                    stringBuilder.append(line + "\n");
                }
            }
            return stringBuilder.toString();

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;

    }




}
