package com.xm4399.test;

import org.apache.kudu.client.*;

public class insertTable {

    public static void main(String[] args) {
        try {
            String tableName = "chenzhikun_test";

            KuduClient client = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").defaultAdminOperationTimeoutMs(60000).build();
            // 获取table
            KuduTable table = client.openTable(tableName);

            // 获取一个会话
            KuduSession session = client.newSession();
            session.setTimeoutMillis(60000);
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(10000);

            // 插入时，初始时间
            long startTime = System.currentTimeMillis();

            int val = 0;
            // 插入数据
            for (int i = 0; i < 60; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();

                row.addLong(0, i);
                row.addLong(1, i*100);
                row.addLong(2, i);
                row.addString(3, "bigData");
                session.apply(insert);
                if (val % 10 == 0) {
                    session.flush();
                    val = 0;
                    /*System.out.println("-------------------------send  10 messages------------------------");
                    sleep(5000);*/
                }
                val++;
            }
            session.flush();
            // 插入时结束时间
            long endTime = System.currentTimeMillis();
            System.out.println("the timePeriod executed is : " + (endTime - startTime) + "ms");
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
