package com.xm4399.run

import org.apache.spark.sql.{SaveMode, SparkSession}

object GudingArgs {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
    // 读取MySQL数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.0.0.211:3307")
      .option("dbtable", "chenzhikun_test.chenzhikun_test_3")
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
      .option("kudu.table", "chenzhikun_test_3")
      .save()

    // 读取写入的数据
    /*spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "pk")
      .load().show()*/
  }


}
