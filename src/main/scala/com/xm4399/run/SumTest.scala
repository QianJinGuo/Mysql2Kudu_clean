package com.xm4399.run

import com.xm4399.realtime.RealTimeIncrease2Kudu
import com.xm4399.util.{CreateKuduTable2, GetTableStru, ListAllSubTableName}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import scala.collection.JavaConverters._

object SumTest {
  def main(args: Array[String]): Unit = {
    val dbName = args(0);
    val tableName = args(1);
    val isSubTable = args(2);
    val isRealTime = args(3);
    val tableStru = GetTableStru.getTableStru(dbName, tableName, isSubTable)
    //获取mysql表的字段名数组,用在分表时的 dataFrame的列位置的调整
    val fieldNameArr :Array[String] = CreateKuduTable2.createKuduTable(tableStru, tableName, isSubTable)
    if ("true".equals(isSubTable)) {
      val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
      val subTableNameList = ListAllSubTableName.listAllSmallTableName(dbName, tableName).asScala
      for(oneSubTableName <- subTableNameList){
        ReadMysqlSubTable2Kudu(spark,dbName, tableName, oneSubTableName,fieldNameArr)
        println("表   "+ oneSubTableName +"加载完毕>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      }
      spark.close()
    }else{
      readMysql2Kudu(dbName,tableName)
    }
    if ("true".equals(isRealTime)){
      new RealTimeIncrease2Kudu().realTimeIncrease(isSubTable);
    }
  }

  def ReadMysqlSubTable2Kudu(spark: SparkSession, dbName: String, tableName : String, oneSubTableName: String, array: Array[String]): Unit = {
    //val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
    // 读取MySQL数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.0.0.211:3307")
      .option("dbtable", dbName + "." + oneSubTableName) //dbName.tableName
      //.option("dbtable", "4399_cnbbs.thread_image_like_user_9")
      .option("user", "gprp")
      .option("password", "gprp@@4399")
      .load()
      .withColumn("table_name",lit(oneSubTableName)) //分表的情况下,kudu表相比于mysql分表,多了table_name主键
    // 调整df列的位置,table_name必须第一列
    val jdbcDF2 = jdbcDF.selectExpr(array:_*)
    val KUDU_MASTERS = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"
    // 将数据过滤后写入Kudu
    jdbcDF2
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", tableName)
      .save()

  }

  def readMysql2Kudu(dbName: String, tableName: String): Unit = {
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

  }

}
