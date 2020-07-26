package com.xm4399.util;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CreateKuduTable {

    //根据传入的字段结构 建kuud表
    public  static void createKuduTable(LinkedHashMap<String,String[]> fieldInfoMap,String tableName){
        KuduClient client = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").defaultAdminOperationTimeoutMs(60000).build();
        KuduSession session = client.newSession();
        // 此处所定义的是rpc连接超时
        session.setTimeoutMillis(60000);
        tableName ="chenzhikun_test_for_SubTable";
        try {
            // 测试，如果table存在的情况下，就删除该表
            if(client.tableExists(tableName)) {
                client.deleteTable(tableName);
                System.out.println("delete the table！");
            }

            List<ColumnSchema> columns = new ArrayList();

            // 创建列
            MysqlType2KuduType mysqlType2KuduType = new MysqlType2KuduType();
            //主键可能有多个,放入数组
            String[] priKeyArr = new String[8];
            int priIndex =0;
            for(Map.Entry<String,String[]>  fieldInfo : fieldInfoMap.entrySet()){
                String name = fieldInfo.getKey();
                String[] fieldInfoArr = fieldInfo.getValue();
                if("PRI".equals(fieldInfoArr[2])){
                    priKeyArr[priIndex] = name;
                    priIndex++;
                    columns.add(new ColumnSchema.ColumnSchemaBuilder(name, mysqlType2KuduType.toKuduType(fieldInfoArr[0])).key(true).build());
                }else if(!("PRI".equals(fieldInfoArr[2])) && "NO".equals(fieldInfoArr[1])){
                    columns.add(new ColumnSchema.ColumnSchemaBuilder(name, mysqlType2KuduType.toKuduType(fieldInfoArr[0])).nullable(false).build());
                }else{
                    columns.add(new ColumnSchema.ColumnSchemaBuilder(name, mysqlType2KuduType.toKuduType(fieldInfoArr[0])).nullable(true).build());
                }
            }

            // 创建schema
            Schema schema = new Schema(columns);
            PriKey2Kudu priKey2Kudu = new PriKey2Kudu();
            ImmutableList<String> hashKeys = priKey2Kudu.getImmutableList(priKeyArr,priIndex);
            CreateTableOptions tableOptions = new CreateTableOptions();

            // 设置hash分区，包括分区数量、副本数目
            tableOptions.addHashPartitions(hashKeys,3);
            tableOptions.setNumReplicas(3);

            // 创建table,并设置partition
            client.createTable(tableName, schema, tableOptions);
            System.out.println("create table is success!");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
//          client.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
