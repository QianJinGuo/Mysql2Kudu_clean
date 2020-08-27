package com.xm4399.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * @Auther: czk
 * @Date: 2020/8/26
 * @Description:
 */
public class CollectUtil {

    public ArrayList<String> listAllMysqlFields(ArrayList<LinkedHashMap<String,String>> fieldsInfoList){
        ArrayList<String> allFieldsList = new ArrayList<String>();
        for (LinkedHashMap<String,String> map : fieldsInfoList){
            for (Map.Entry<String,String>  entry : map.entrySet()){
                String field = entry.getKey();
                System.out.println("allFiledsList存入 "+field);
                allFieldsList.add(field);
            }
        }
        return allFieldsList;
    }

    public ArrayList<String> listMysqlPriKey(ArrayList<LinkedHashMap<String,String>> fieldsInfoList){
        ArrayList<String> priKeyList = new ArrayList<String>();
        for (LinkedHashMap<String,String> map : fieldsInfoList){
            for (Map.Entry<String,String>  entry : map.entrySet()){
                String isRtiKey = entry.getValue();
                String field = entry.getKey();
                if ("PRI".equals(isRtiKey)){
                    System.out.println("mysql主键有>>>"+ field);
                    priKeyList.add(field);
                }

            }
        }
        return priKeyList;
    }

    // 比较两个list是否相同
    public boolean isSameForTwoLists(ArrayList<String> list, ArrayList<String> list1) {
        if(list.size() != list1.size()) {
            return false;
        }
        for(String str : list) {
            if(!list1.contains(str)) {
                return false;
            }
        }
        return true;
    }


}
