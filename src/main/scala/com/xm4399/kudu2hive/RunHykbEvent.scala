package com.xm4399.kudu2hive


import org.apache.spark.sql.SparkSession
/**
  *
  * @note
  * @author lxc
  *
  *         date  : 19-4-18
  *
  **/
object RunHykbEvent {
   /* def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)
        val spark = SparkSession.builder()
          .appName("hykb")
          .enableHiveSupport()
          .getOrCreate()

        spark.sql("set hive.parquet_optimized_reader_enabled=true")
        spark.sql("set hive.parquet_predicate_pushdown_enabled = true")
        spark.sql("set  hive.exec.dynamici.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        val inputPath: String = args(0)
        val database = args(1)
        val hykb = new HykbEvent(spark, database, inputPath)

        args.length match {
            case 4 => {
                val beginDate: String = args(2)
                val endDate: String = args(3)
                LogsRun.runEveryDay(hykb, beginDate, endDate, inputPath, database)
            }
            case _ => {
                println("Error args")
                throw new RuntimeException("Error args!")

            }
        }
        spark.stop()
    }*/
}
