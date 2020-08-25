package com.xm4399.run

import org.apache.spark.sql.SparkSession

/**
 * @Auther: czk
 * @Date: 2020/8/25 
 * @Description:
 */
object Row {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GetKuduTableRowsCount").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + "10.0.0.92:3310")
      .option("dbtable", "4399_cnbbs.thread_image_like_user_0") //dbName.tableName
      .option("user", "cnbbsReadonly")
      .option("password", "LLKFN*k241235")
      .load()
      .sample(false,0.001)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    print(jdbcDF.count())
    //Thread.sleep(20*60*1000)
    val kuduDF = spark.read
      .options(Map("kudu.master" -> "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051",
        "kudu.table" -> "czk_num_flase_false"))
      .format("kudu")
      .load
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(kuduDF.count())

    


  }

}
