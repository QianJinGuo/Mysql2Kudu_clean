package com.xm4399.util;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: czk
 * @Date: 2020/8/27
 * @Description:
 */
public class KuduUtil {
    // 获取主键list
    public ArrayList<String>  listKuduPriKey(KuduTable kuduTable){
        ArrayList<String> kuduPriKeyList = new ArrayList<String>();
        Schema colSchema = kuduTable.getSchema();
        List<ColumnSchema> pkList = colSchema.getPrimaryKeyColumns();
        for(ColumnSchema item : pkList){
            String colName = item.getName();
            kuduPriKeyList.add(colName);
        }
        return kuduPriKeyList;
    }

    //获取所有字段list
    public ArrayList<String>  listAllKuduCol(KuduTable kuduTable){
        ArrayList<String> allKuduColList = new ArrayList<String>();
        Schema colSchema = kuduTable.getSchema();
        List<ColumnSchema> pkList = colSchema.getColumns();
        for(ColumnSchema item : pkList){
            String colName = item.getName();
            allKuduColList.add(colName);
        }
        return allKuduColList;
    }


}
