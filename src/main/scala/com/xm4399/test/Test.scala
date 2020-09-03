package com.xm4399.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

/**
 * @Auther: czk
 * @Date: 2020/9/2 
 * @Description:
 */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("MysqlFullPullKudu").getOrCreate()
    var jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + "localhost:3306")
      .option("dbtable", "chenzhikun.first_canal") //dbName.tableName
      //.option("dbtable", "4399_cnbbs.thread_image_like_user_9")
      .option("user", "canal")
      .option("password", "canal")
      .load()
    jdbcDF.show()
    val s = jdbcDF.withColumn("id",substring(col("id"),0,1))
      //.drop("name").withColumnRenamed("newColumn","name")
    s.show()
  }
}
