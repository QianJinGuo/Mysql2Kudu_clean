package com.xm4399.test

import org.apache.spark.sql.SparkSession

/**
 * @Auther: czk
 * @Date: 2020/8/26 
 * @Description:
 */
object dfColumnTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("MysqlFullPullKudu").getOrCreate()
    var jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + "localhost:3306")
      .option("dbtable", "chenzhikun.first_canal") //dbName.tableName
      .option("user", "canal")
      .option("password", "canal")
      .load()
    jdbcDF.show()
    
  }

}
