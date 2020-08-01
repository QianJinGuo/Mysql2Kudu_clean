package com.xm4399.util;

import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;

public class MysqlPriKey2KuduPriKey {


    public ImmutableList<String> getImmutableList(String[] priKeyArr,int priNum){
        switch (priNum){
            case 1:
                return  ImmutableList.of(priKeyArr[0]);
            case 2:
                return  ImmutableList.of(priKeyArr[0],priKeyArr[1]);
            case 3:
                return  ImmutableList.of(priKeyArr[0],priKeyArr[1],priKeyArr[2]);
            case 4:
                return  ImmutableList.of(priKeyArr[0],priKeyArr[1],priKeyArr[2],priKeyArr[3]);
            case 5:
                return  ImmutableList.of(priKeyArr[0],priKeyArr[1],priKeyArr[2],priKeyArr[3],priKeyArr[4]);
            case 6:
                return  ImmutableList.of(priKeyArr[0],priKeyArr[1],priKeyArr[2],priKeyArr[3],priKeyArr[4],priKeyArr[5]);
            case 0:
                System.out.println("该表没有主键");
                return ImmutableList.of(priKeyArr[0]);
            default:
                throw new IllegalArgumentException("The table doesn't have primarykey.");

        }
       //return ImmutableList.of(priKeyArr[0]);
    }
}
