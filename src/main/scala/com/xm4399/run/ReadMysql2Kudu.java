package com.xm4399.run;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @Auther: czk
 * @Date: 2020/7/31
 * @Description:
 */
public class ReadMysql2Kudu {
    /*def readMysql2Kudu(dbName: String, tableName: String): Unit = {
        val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
        // 读取MySQL数据
        val jdbcDF = spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://10.0.0.211:3307")
                .option("dbtable", dbName + "." + tableName) //dbName.tableName
                //.option("dbtable", "4399_cnbbs.thread_image_like_user_9")
                .option("user", "gprp")
                .option("password", "gprp@@4399")
                .load()
        val KUDU_MASTERS = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"
        // 将数据过滤后写入Kudu
        jdbcDF
                .write
                .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", KUDU_MASTERS)
                .option("kudu.table", tableName)
                .save()
        spark.close()

    }*/
    public static  void readMysql2Kudu (String dbName, String tableName){
        SparkSession spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate();
        String kuduMaster = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051";
        spark.read()
                .format("jdbc")
                .option("url","jdbc:mysql://10.0.0.211:3307")
                .option("datable",dbName + "." + tableName)
                .option("user","gprp")
                .option("password","gprp@@4399")
                .load()
                .write()
                .mode(SaveMode.Append)
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", kuduMaster)
                .option("kudu.table", tableName)
                .save();
        spark.close();
    }

    public static void main(String[] args) {
        readMysql2Kudu("chenzhikun_test","quanziInfo_2");
    }
}
