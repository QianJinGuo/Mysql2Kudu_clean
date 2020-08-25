package com.xm4399.run

import org.apache.kudu.spark.kudu._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by angel；
 */
object KuduTableRows {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GetKuduTableRowsCount").getOrCreate()
    // Read a table from Kudu
    val df = spark.read
      .options(Map("kudu.master" -> "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051",
        "kudu.table" -> "czk_num_flase_false"))
      .format("kudu")
      .load
      .filter("dateline < 1598346300 ")

    val count = df.count()
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println("kudu rows >>"+count)
    spark.close()


  }

}
