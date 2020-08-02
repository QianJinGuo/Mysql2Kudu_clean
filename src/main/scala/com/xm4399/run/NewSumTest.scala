package com.xm4399.run

import com.xm4399.realtime.RealTimeIncrease2Kudu
import com.xm4399.run.SumTest.{ReadMysqlSubTable2Kudu, readMysql2Kudu}
import com.xm4399.util.{CreateKuduTable2, GetTableStru, ListAllSubTableName}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/7/31 
 * @Description:
 */
object NewSumTest {

  def main(args: Array[String]): Unit = {
    val address = args(0);
    println(address)
    val username = args(1);
    val password = args(2);
    val dbName = args(3);
    val tableName = args(4);
    val isSubTable = args(5);
    val isRealtime = args(6);
    val tableStru = GetTableStru.getTableStru2(address, username, password, dbName, tableName, isSubTable)
    //获取mysql表的字段名数组,用在分表时的 dataFrame的列位置的调整
    val fieldNameArr :Array[String] = CreateKuduTable2.createKuduTable(tableStru, tableName, isSubTable)
    if ("true".equals(isSubTable)) {
      val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
      val subTableNameList = ListAllSubTableName.listAllSmallTableName2(address, username, password, dbName, tableName).asScala
      for(oneSubTableName <- subTableNameList){
        println("表   "+ oneSubTableName +"正要加载>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        ReadMysqlSubTable2Kudu(spark, address, username, password, dbName, tableName, oneSubTableName, fieldNameArr)
        println("表   "+ oneSubTableName +"加载完毕>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      }
      spark.close()
    }else{
      readMysql2Kudu(address, username, password, dbName,tableName)
    }
    if (!"false".equals(isRealtime)){
      new RealTimeIncrease2Kudu().realTimeIncrease(isSubTable);
    }

  }

  def ReadMysqlSubTable2Kudu(spark: SparkSession, address: String, username: String, password: String, dbName: String, tableName : String, oneSubTableName: String, array: Array[String]): Unit = {
    //val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
    // 获取分表的table_id
    val table_id_str = oneSubTableName.substring(oneSubTableName.lastIndexOf("_") + 1, oneSubTableName.length)
    val table_id = table_id_str.toShort
    // 读取MySQL数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + oneSubTableName) //dbName.tableName
      //.option("dbtable", "4399_cnbbs.thread_image_like_user_9")
      .option("user", username)
      .option("password", password)
      .load()
      .withColumn("table_id",lit(table_id)) //分表的情况下,kudu表相比于mysql分表,多了table_name主键
    // 调整df列的位置,table_name必须第一列
    val jdbcDF2 = jdbcDF.selectExpr(array:_*)
    val KUDU_MASTERS = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"
    // 将数据过滤后写入Kudu
    jdbcDF2
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "chenzhikun_test_for_SubTable")
      .save()

  }

  def readMysql2Kudu(address: String, username: String, password: String, dbName: String, tableName: String): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
    // 读取MySQL数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + tableName) //dbName.tableName
      //.option("dbtable", "4399_cnbbs.thread_image_like_user_9")
      .option("user", username)
      .option("password", password)
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
    spark.close()

  }

}