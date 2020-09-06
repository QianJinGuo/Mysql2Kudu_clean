package com.xm4399.test

import com.xm4399.util.{ConfUtil, JDBCOnlineUtil}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, substring}
import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/9/2 
 * @Description:
 */
object Test {
  def main(args: Array[String]): Unit = {
    val fields = "id,tid"
    val spark = SparkSession.builder().appName("MysqlFullPull2Kudu_434" ).getOrCreate()
    var jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + "10.0.0.92:3310")
      .option("dbtable", "4399_cnbbs.thread_image_like_user_0") //dbName.tableName
      .option("user", "cnbbsReadonly")
      .option("password", "LLKFN*k241235")
      .load()

    //分全表拉取和按字段拉取
    if (!"false".equals(fields)){
      val fieldsArr = fields.split(",");
      jdbcDF = jdbcDF.selectExpr(fieldsArr:_*)
    }

   /* //如果mysql表有mediumtext或longtext类型字段,其值的字节长度可能大于kudu的64k,对其截断
    val longTextFiledsList = new JDBCOnlineUtil().listLongTextFields(address,username,password,dbName,tableName)
    if (longTextFiledsList.size() >0){
      val list = longTextFiledsList.asScala
      for (columnName <- list){
        // 对于text类型的字段,进行截取字符串
        jdbcDF = jdbcDF.withColumn(columnName,substring(col(columnName),0,16380))
      }
    }*/

    val KUDU_MASTERS = new ConfUtil().getValue("kuduMaster")
    jdbcDF
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "default.czk_test_fields")
      .save()
    spark.close()
  }

}
