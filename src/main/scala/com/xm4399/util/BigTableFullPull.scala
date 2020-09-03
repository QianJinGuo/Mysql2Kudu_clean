package com.xm4399.util

import java.util.Properties

import org.apache.spark.sql.functions.{col, lit, substring}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/9/2
 * @Description:
 */
object BigTableFullPull {

  def main(args: Array[String]): Unit = {
    val address = args(0)
    val username = args(1)
    val password = args(2)
    val dbName = args(3)
    val tableName = args(4)
    val fields = args(5)
    val kuduTableName = args(6)
    pullBigTable(address, username, password, dbName, tableName, fields, kuduTableName)
  }

  def pullBigTable(address : String, username : String, password : String, dbName : String,
                   tableName : String, fields : String, kuduTableName : String): Unit = {
    val spark = SparkSession.builder().appName("MysqlFullPullKudu").getOrCreate()
    // 获取mysql主键
    val map = new JDBCOnlineUtil().getTablePriKeyStru(address,username,password,dbName,tableName)
    val mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    var pkStr : String = null
    if (mysqlPriKeyList.size() > 0){
      pkStr = mysqlPriKeyList.get(0)
    }
    // 分区设置
    val partitons = new Array[String](3);
    //三个分区,第一个为1-500w,第二为500w-1000w,第三为1000为2000w.多出的50为缓解拉取过程中的增量数据
    partitons(0) = s"1=1 and $pkStr >=(select $pkStr from $tableName order by $pkStr limit 1) limit  5000000"
    partitons(1) = s"1=1 and $pkStr >=(select $pkStr from $tableName order by $pkStr limit 4999950,1) limit  5000100"
    partitons(2) = s"1=1 and $pkStr >=(select $pkStr from $tableName order by $pkStr limit 9999950,1) limit  0,10000000"

    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", username)
    prop.setProperty("password" , password )
    var jdbcDF = spark.read.jdbc(s"jdbc:mysql://$address/$dbName", tableName, partitons, prop)

    //有mediumtext或longtext类型字段的mysql表,其值的字节长度可能大于kudu的64k,对其截断
    val longTextFiledsList = new JDBCOnlineUtil().listLongTextFields(address,username,password,dbName,tableName)
    if (longTextFiledsList.size() >0){
      val list = longTextFiledsList.asScala
      for (columnName <- list){
        // 增加新列存蓄截断后的字符值,删除原来字段,重命名新字段为原来字段名
        jdbcDF = jdbcDF.withColumn("newColumn",substring(col(columnName),0,16380))
          .drop(columnName).withColumnRenamed("newColumn",columnName)
      }
     /* val filedsList = new JDBCOnlineUtil().listAllFields(address, username, password, dbName, tableName).asScala
      jdbcDF.selectExpr(filedsList:_*)*/
    }

    //分全表拉取和按字段拉取
    if (!"false".equals(fields)){
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
  }

  def pullBigTableSubTable(spark : SparkSession,address : String, username : String, password : String, dbName : String,
                           oneSubTableName : String, fields : String, kuduTableName : String): Unit = {
    // 获取mysql主键
    val map = new JDBCOnlineUtil().getTablePriKeyStru(address,username,password,dbName,oneSubTableName)
    val mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    var pkStr : String = null
    if (mysqlPriKeyList.size() > 0){
      pkStr = mysqlPriKeyList.get(0)
    }
    // 分区设置
    val partitons = new Array[String](3);
    //三个分区,第一个为1-500w,第二为500w-1000w,第三为1000为2000w.多出的50为缓解拉取过程中的增量数据
    partitons(0) = s"1=1 and $pkStr >=(select $pkStr from $oneSubTableName order by $pkStr limit 1) limit  5000000"
    partitons(1) = s"1=1 and $pkStr >=(select $pkStr from $oneSubTableName order by $pkStr limit 4999950,1) limit  5000100"
    partitons(2) = s"1=1 and $pkStr >=(select $pkStr from $oneSubTableName order by $pkStr limit 9999950,1) limit  0,10000000"

    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", username)
    prop.setProperty("password" , password )
    // 获取分表的table_id
    val table_id_str = oneSubTableName.substring(oneSubTableName.lastIndexOf("_") + 1, oneSubTableName.length)
    val table_id = table_id_str.toShort
    var jdbcDF = spark.read
      .jdbc(s"jdbc:mysql://$address/$dbName", oneSubTableName, partitons, prop)
      .withColumn("table_id",lit(table_id))

    //有mediumtext或longtext类型字段的mysql表,其值的字节长度可能大于kudu的64k,对其截断
    val longTextFiledsList = new JDBCOnlineUtil().listLongTextFields(address,username,password,dbName,oneSubTableName)
    if (longTextFiledsList.size() >0){
      val list = longTextFiledsList.asScala
      for (columnName <- list){
        // 增加新列存蓄截断后的字符值,删除原来字段,重命名新字段为原来字段名
        jdbcDF = jdbcDF.withColumn("newColumn",substring(col(columnName),0,16380))
          .drop(columnName).withColumnRenamed("newColumn",columnName)
      }
      /* val filedsList = new JDBCOnlineUtil().listAllFields(address, username, password, dbName, tableName).asScala
       jdbcDF.selectExpr(filedsList:_*)*/
    }

    //分全表拉取和按字段拉取
    if (!"false".equals(fields)){
      val fieldsArr = fields.split(",");
      val tableidField : Array[String] = Array("table_id")
      val allFieldsArr = fieldsArr ++ tableidField
      jdbcDF = jdbcDF.selectExpr(allFieldsArr:_*)

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
  }

}
