package com.xm4399.run

import org.apache.kudu.spark.kudu._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by angel；
 */
object KuduTableRows {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("getKuduTableRowCount")
      //设置Master_IP并设置spark参数
      .setMaster("local")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.network.timeout", "600s")
      .set("spark.task.maxFailures", "1")
      .set("spark.speculationfalse", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    /*    //TODO 1:定义表名
        val kuduTableName = "czk_num_flase_false"
        val kuduMasters = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"
        //使用spark创建kudu表
        val kuduContext = new KuduContext(kuduTableName, sqlContext.sparkContext)

        //TODO 2：配置kudu参数
        val kuduOptions: Map[String, String] = Map(
          "kudu.table"  -> kuduTableName,
          "kudu.master" -> kuduMasters)
        //TODO 3：执行读取操作
        val customerReadDF = sqlContext.read.options(kuduOptions).kudu
        val filterData = customerReadDF.select("name" ,"age", "city").filter("age<30")
        //TODO 4：打印
        filterData.show()*/
    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> "czk_num_flase_false",
      "kudu.master" -> "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
    val reader: DataFrame = sqlContext.read.options(kuduOptions).kudu
    val rowCount = reader.count()
    print(rowCount)
  }
}
