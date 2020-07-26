package com.xm4399.run

import com.xm4399.test.MyTsetCreate
import com.xm4399.util.{CreateKuduTable, CreateKuduTable2, ListAllSubTableName, GetTableStru, GetTableStru2}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._
object MyTest {
  def main(args: Array[String]): Unit = {

    val argsLen = args.length
    val tableStru = GetTableStru2.getTableStru(args(0), args(1), argsLen) //JdbcTest.getDBTableStru
    //获取mysql表的字段名数组,用在分表时的 dataFrame的列位置的调整
    val fieldNameArr :Array[String] = CreateKuduTable2.createKuduTable(tableStru, args(1),argsLen)

    if (3 == argsLen) {
      val smallTableNameList = ListAllSubTableName.getAllSmallTableName(args(0), args(1)).asScala
      for(oneSmallTable <- smallTableNameList){
        println("正在加载表   "+ oneSmallTable +">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        ReadMysqlSubTable2Kudu(args(0), oneSmallTable,fieldNameArr)
        println("表   "+ oneSmallTable +"加载完毕>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      }
    }else{
      readMysql2Kudu(args(0),args(1))
    }
  }

  def ReadMysqlSubTable2Kudu(dbName: String, tableName: String, array: Array[String]): Unit = {
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
      .withColumn("table_name",lit(tableName)) //分表的情况下,kudu表相比于mysql分表,多了table_name这个主键



    //调整df列的位置,table_name必须第一列
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
        .option("kudu.table", "chenzhikun_test_for_SubTable")
        .save()
    }



}
