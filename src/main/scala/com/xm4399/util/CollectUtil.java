package com.xm4399.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Auther: czk
 * @Date: 2020/8/26
 * @Description:
 */
public class CollectUtil {

    public ArrayList<String> listAllMysqlFields(LinkedHashMap<String,String> fieldAndIsPKMap){
        ArrayList<String> allFieldsList = new ArrayList<String>();
        for (Map.Entry<String,String>  fieldInfo : fieldAndIsPKMap.entrySet()){
            allFieldsList.add(fieldInfo.getKey());
        }
        return allFieldsList;
    }

    public ArrayList<String> listMysqlPriKey(LinkedHashMap<String,String> fieldAndIsPKMap ){
        ArrayList<String> priKeyList = new ArrayList<String>();
        for (Map.Entry<String,String>  fieldInfo : fieldAndIsPKMap.entrySet()){
            if ("PRI".equals(fieldInfo.getValue())){
                priKeyList.add(fieldInfo.getKey());
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
