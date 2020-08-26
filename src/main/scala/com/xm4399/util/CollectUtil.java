package com.xm4399.util;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * @Auther: czk
 * @Date: 2020/8/26
 * @Description:
 */
public class CollectUtil {

    public LinkedList<String> listAllFields (LinkedList<LinkedHashMap<String,String>> fieldsInfoList){
        LinkedList<String> allFieldsList = new LinkedList<String>();
        for (LinkedHashMap<String,String> map : fieldsInfoList){
            for (Map.Entry<String,String>  entry : map.entrySet()){
                String field = entry.getKey();
                allFieldsList.add(field);
            }
        }
        return allFieldsList;
    }

    public LinkedList<String> listAllPriKey (LinkedList<LinkedHashMap<String,String>> fieldsInfoList){
        LinkedList<String> allPriKeyList = new LinkedList<String>();
        for (LinkedHashMap<String,String> map : fieldsInfoList){
            for (Map.Entry<String,String>  entry : map.entrySet()){
                String field = entry.getKey();
                if ("PRI".equals(field)){
                    allPriKeyList.add(field);
                }

            }
        }
        return allPriKeyList;
    }

}
