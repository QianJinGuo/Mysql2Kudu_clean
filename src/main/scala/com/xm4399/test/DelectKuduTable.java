package com.xm4399.test;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;

public class DelectKuduTable {
    public static void main(String[] args) throws KuduException {
        KuduClient client = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
                .defaultAdminOperationTimeoutMs(60000).build();
        KuduSession session = client.newSession();
        // 此处所定义的是rpc连接超时
        session.setTimeoutMillis(60000);
        for(String tableName : args){
            client.deleteTable(tableName);
            System.out.println("delete the table>>>>>>>>>>>>" + tableName);
        }
    }
}
