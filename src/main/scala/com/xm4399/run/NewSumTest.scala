package com.xm4399.run

import com.xm4399.util.{CreateKuduTable2, JDBCUtil, ListAllSubTableName}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
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
    val jobID= args(0)
    val confInfoArr = JDBCUtil.getConfInfoArr(jobID)
    val address = confInfoArr(0);
    val username = confInfoArr(1);
    val password = confInfoArr(2);
    val dbName = confInfoArr(3);
    val tableName = confInfoArr(4);
    val isSubTable = confInfoArr(6);
    val kuduTableName = confInfoArr(8)
    val fields = confInfoArr(9)


    if ("true".equals(isSubTable)) {
      try {
        // 记录全量拉取running状态
        JDBCUtil.updateRunningFullPull(jobID)
        val fieldNameArr = CreateKuduTable2.listKuduFieldName(kuduTableName).asScala.toList
        val spark = SparkSession.builder().appName("MysqlFullPullKudu").getOrCreate()
        val subTableNameList = ListAllSubTableName.listAllSubTableName(address, username, password, dbName, tableName).asScala
        for(oneSubTableName <- subTableNameList){
          println("表   "+ oneSubTableName +"正要加载>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          ReadMysqlSubTable2Kudu(spark, address, username, password, dbName, tableName, oneSubTableName, fieldNameArr, kuduTableName, fields)
          println("表   "+ oneSubTableName +"加载完毕>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        }
        // 记录全量拉取完成的状态
        JDBCUtil.updateFullPull(jobID)
        spark.close()
      } catch {
        case e : Exception => JDBCUtil.updateExceptionFullPull(jobID)
      }

    }else{
      try{
        // 记录全量拉取running状态
        JDBCUtil.updateRunningFullPull(jobID)
        readMysql2Kudu(address, username, password, dbName,tableName, kuduTableName,  fields, jobID)
      } catch {
          case e : Exception => JDBCUtil.updateExceptionFullPull(jobID)
      }
    }
  }

  def ReadMysqlSubTable2Kudu(spark: SparkSession, address: String, username: String, password: String, dbName: String, tableName : String, oneSubTableName: String,
                             fieldNameList:  List[String], kuduTableName :String, fields :String): Unit = {
    // 获取分表的table_id
    val table_id_str = oneSubTableName.substring(oneSubTableName.lastIndexOf("_") + 1, oneSubTableName.length)
    val table_id = table_id_str.toShort
    // 读取MySQL数据
    var jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + oneSubTableName) //dbName.tableName
      //.option("dbtable", "4399_cnbbs.thread_image_like_user_9")
      .option("user", username)
      .option("password", password)
      .load()
      .withColumn("table_id",lit(table_id)) //分表的情况下,kudu表相比于mysql分表,多了table_name主键

    if ("false".equals(fields)){
      val fieldsArr :Array[String] =  fields.split(",")
      val tableidField : Array[String] = Array("table_id")
      val allFieldsArr = fieldsArr ++ tableidField
      jdbcDF = jdbcDF.selectExpr(allFieldsArr:_*)

    } else {
      jdbcDF = jdbcDF.selectExpr(fieldNameList:_*)
    }

    // 调整df列的位置,table_name必须第一列
   // val jdbcDF2 = jdbcDF.selectExpr(fieldNameList:_*)
    val KUDU_MASTERS = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"
    // 将数据过滤后写入Kudu
    jdbcDF
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", kuduTableName)
      .save()
  }

  def readMysql2Kudu(address: String, username: String, password: String, dbName: String, tableName: String, kuduTableName: String, fields :String, jobID : String): Unit = {
    //val spark = SparkSession.builder().appName("MysqlFullPullKudu").getOrCreate()
    val spark = SparkSession.builder().appName("MysqlFullPullKudu").getOrCreate()
    // 读取MySQL数据
    var jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + tableName) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()
    if ("false".equals(fields)){
      val fieldsArr = fields.split(",");
      jdbcDF = jdbcDF.selectExpr(fieldsArr:_*)
    }

    val KUDU_MASTERS = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"

    jdbcDF
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", kuduTableName)
      .save()
    spark.close()
    JDBCUtil.updateFullPull(jobID)

  }

}
