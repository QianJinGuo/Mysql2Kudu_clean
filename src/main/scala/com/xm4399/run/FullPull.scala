package com.xm4399.run

import com.xm4399.util.{BigTableFullPull, ConfUtil, CreateKuduTable, JDBCOnlineUtil, JDBCUtil, OtherUtil}
import org.apache.spark.sql.functions.{col, lit, substring}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/7/31 
 * @Description:
 */
object FullPull {

  def main(args: Array[String]): Unit = {
    val jobID= args(0)
    val confInfoArr = new JDBCUtil().getConfInfoArr(jobID)
    println(confInfoArr.length)
    val address = confInfoArr(0);
    val username = confInfoArr(1);
    val password = confInfoArr(2);
    val dbName = confInfoArr(3);
    val tableName = confInfoArr(4);
    val fields = confInfoArr(5)
    val isSubTable = confInfoArr(6);
    val kuduTableName = confInfoArr(8)
    val isBigTable = confInfoArr(11)
   // --------------------------------------------------------------------
    try {
      // 记录全量拉取running状态
      new JDBCUtil().updateJobState(jobID, "2_FullPull");
      // 是否大表,是否分表
      if ("false".equals(isBigTable)){
        if ("true".equals(isSubTable)) {
          val fieldNameArr = CreateKuduTable.listKuduFieldName(kuduTableName).asScala.toList
          val spark = SparkSession.builder().appName("MysqlFullPull2Kudu_" +jobID).getOrCreate()
          val subTableNameList = new JDBCOnlineUtil().listAllSubTableName(address, username, password, dbName, tableName).asScala
          for(oneSubTableName <- subTableNameList){
            pullSubTable(spark, address, username, password, dbName, tableName, oneSubTableName, fieldNameArr, kuduTableName, fields)
          }
          spark.close()
        }else {
          pullTable(address, username, password, dbName,tableName, kuduTableName,  fields, jobID)
        }
      }else if ("true".equals(isBigTable)){
        if ("true".equals(isSubTable)) {
          val spark = SparkSession.builder().appName("MysqlFullPull2Kudu_" + jobID).getOrCreate()
          val subTableNameList = new JDBCOnlineUtil().listAllSubTableName(address, username, password, dbName, tableName).asScala
          for(oneSubTableName <- subTableNameList){
            BigTableFullPull.pullBigTableSubTable(spark, address, username, password, dbName, oneSubTableName, fields, kuduTableName)
          }
          spark.close()
        }else {
          BigTableFullPull.pullBigTable(jobID,address, username, password, dbName,tableName, fields, kuduTableName)
        }
      }
      // 记录全量拉取完成的状态
      new JDBCUtil().updateJobState(jobID, "1_FullPull");
    }catch {
        case e : Exception => {
        new JDBCUtil().updateJobState(jobID, "-1_FullPull");
        new JDBCUtil().insertErrorInfo(jobID, "FullPull", "" )
        e.printStackTrace()
      }
    }

  }

  def pullSubTable(spark: SparkSession, address: String, username: String, password: String, dbName: String, tableName : String,
                   oneSubTableName: String, fieldNameList:  List[String], kuduTableName :String, fields :String): Unit = {
    // 获取分表的table_id
    val table_id_str = oneSubTableName.substring(oneSubTableName.lastIndexOf("_") + 1, oneSubTableName.length)
    val table_id = table_id_str.toShort
    // 读取MySQL数据
    var jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + oneSubTableName)
      .option("user", username)
      .option("password", password)
      .load()
      .withColumn("table_id",lit(table_id)) //分表的情况下,kudu表相比于mysql分表,多了table_name主键
    // 分全表拉取和按字段拉取
    if (!"false".equals(fields)){
      val fieldsArr :Array[String] =  fields.split(",")
      val tableidField : Array[String] = Array("table_id")
      val allFieldsArr = fieldsArr ++ tableidField
      jdbcDF = jdbcDF.selectExpr(allFieldsArr:_*)
    } /*else {
      jdbcDF = jdbcDF.selectExpr(fieldNameList:_*)
    }*/

    //如果mysql表有mediumtext或longtext类型字段,其值的字节长度可能大于kudu的64k,对其截断
    val longTextFiledsList = new JDBCOnlineUtil().listLongTextFields(address,username,password,dbName,tableName)
    if (longTextFiledsList.size() >0){
      val list = longTextFiledsList.asScala
      for (columnName <- list){
        // 对于text类型的字段,进行截取字符串
        jdbcDF = jdbcDF.withColumn(columnName,substring(col(columnName),0,16380))
      }
      /* val filedsList = new JDBCOnlineUtil().listAllFields(address, username, password, dbName, tableName).asScala
       jdbcDF.selectExpr(filedsList:_*)*/
    }

    val KUDU_MASTERS = new ConfUtil().getValue("kuduMaster")
    jdbcDF
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", kuduTableName)
      .save()
  }

  def pullTable(address: String, username: String, password: String, dbName: String,
                tableName: String, kuduTableName: String, fields :String, jobID : String): Unit = {
    val spark = SparkSession.builder().appName("MysqlFullPull2Kudu_" + jobID).getOrCreate()
    var jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + tableName) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()

    //分全表拉取和按字段拉取
    if (!"false".equals(fields)){
      val fieldsArr = fields.split(",");
      jdbcDF = jdbcDF.selectExpr(fieldsArr:_*)
    }

    //如果mysql表有mediumtext或longtext类型字段,其值的字节长度可能大于kudu的64k,对其截断
    val longTextFiledsList = new JDBCOnlineUtil().listLongTextFields(address,username,password,dbName,tableName)
    if (longTextFiledsList.size() >0){
      val list = longTextFiledsList.asScala
      for (columnName <- list){
        // 对于text类型的字段,进行截取字符串
        jdbcDF = jdbcDF.withColumn(columnName,substring(col(columnName),0,16380))
      }
    }

    val KUDU_MASTERS = new ConfUtil().getValue("kuduMaster")
    jdbcDF
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", kuduTableName)
      .save()
    spark.close()
  }



}
