package com.xm4399.run

import java.util
import java.util.{ArrayList, LinkedHashMap}

import com.xm4399.test.JdbcTest
import com.xm4399.util.{CreateKuduTable, ListAllSubTableName, GetTableStru}
import org.apache.spark.sql.{SaveMode, SparkSession}
import  scala.collection.JavaConverters._


object Test {


    def main(args: Array[String]): Unit = {
      val argsLen = args.length

      val tableStru = GetTableStru.getTableStru(args(0), args(1), argsLen) //JdbcTest.getDBTableStru
      CreateKuduTable.createKuduTable(tableStru, args(1))

      if (3 == argsLen) {
        val smallTableNameList = ListAllSubTableName.getAllSmallTableName(args(0), args(1)).asScala
        for(oneSmallTable <- smallTableNameList){
          readMysql2Kudu(args(0), oneSmallTable)
        }
      }else{
        readMysql2Kudu(args(0),args(1))
      }

}
    def readMysql2Kudu(dbName: String, tableName: String): Unit = {
      val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
      // 读取MySQL数据
      val jdbcDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://10.0.0.92:3310")
        .option("dbtable", dbName + "." + tableName) //dbName.tableName
        .option("user", "cnbbsReadonly")
        .option("password", "LLKFN*k241235")
        .load()
      val KUDU_MASTERS = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"

      // 将数据过滤后写入Kudu
      jdbcDF
        .write
        .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
        .format("org.apache.kudu.spark.kudu")
        .option("kudu.master", KUDU_MASTERS)
        .option("kudu.table", "chenzhikun_test_for_SubTable")
        .save()
    }

  }


/* val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
            // 读取MySQL数据
            val jdbcDF = spark.read
              .format("jdbc")
              .option("url", "jdbc:mysql://10.0.0.211:3307")
              .option("dbtable", args(0) + "." + args(1) )  //dbName.tableName
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
              .option("kudu.table", args(1))
              .save()*/

// 读取写入的数据
/*spark.read.format("org.apache.kudu.spark.kudu")
  .option("kudu.master", KUDU_MASTERS)
  .option("kudu.table", "pk")
  .load().show()*/

