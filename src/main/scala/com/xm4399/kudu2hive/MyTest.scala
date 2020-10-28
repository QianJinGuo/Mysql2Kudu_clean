package com.xm4399.kudu2hive

import com.xm4399.util2.MysqlSchemaUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}
/**
 * @Auther: czk
 * @Date: 2020/10/22 
 * @Description:
 */
object MyTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UpdateEventScoreChangeData")
      .enableHiveSupport()
      //.config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()
    spark.sql("set hive.parquet_optimized_reader_enabled=true")
    spark.sql("set hive.parquet_predicate_pushdown_enabled = true")
    spark.sql("set  hive.exec.dynamici.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    import spark.implicits._
    import spark.sql

    import org.apache.spark.sql.functions._

    val is_server = true
    val schema :StructType = MysqlSchemaUtil.getMysqlSchema("")

    var df = spark.read.format("org.apache.kudu.spark.kudu")
      .options(Map("kudu.master" -> "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051", "kudu.table" -> "kuaibao_mysql.kb_ugc_user_topic"))
      .load
      .filter("score_update_date = 20201027")
      .drop("score_update_date")
      .drop("time")
      .drop("width")
      .drop("prev_score")
      .withColumnRenamed("video_count", "video_num")
      .withColumnRenamed("tid","post_id")
      .withColumn("is_server", lit(is_server))
    df.show(20)




    val hiveTableFieldsArr = schema.fields
    val kuduDfFieldsList = df.columns.toList
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>hive size" + hiveTableFieldsArr.length)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>kudu size" + kuduDfFieldsList.size)

    var hiveList : List[String] = List()
    for (field <- hiveTableFieldsArr){
      val fieldName = field.name
      hiveList = fieldName +:hiveList
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>1111  " + fieldName)
      if (!kuduDfFieldsList.contains(fieldName)){
        if (!"datekey".equals(fieldName) || "event".equals(fieldName)){}
        val fieldType = field.dataType
        println(">>>>>>>>>>>>>>>>>>>>in add col")
        df = df.withColumn(fieldName, lit(null))
      }
    }

    val DfFieldsList = df.columns.toList
    for (ll <- DfFieldsList){
      if (!hiveList.contains(ll)){
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>. not in  kudu" + ll)
      }
    }
    for (fieldName <- DfFieldsList){
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>2222  " + fieldName)
    }
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>after kudu size" + DfFieldsList.size)
    df = df.drop("datekey").drop("event")
    df.createOrReplaceTempView("temp_table")
    df.show(50)
    //切换hive的数据库
    sql("use default")
    spark.sql("insert into default.czk_hykb_test partition(`datekey`='20201026',`event`='score_change') select * from temp_table")
    spark.stop



    //    1、创建分区表，可以将append改为overwrite，这样如果表已存在会删掉之前的表，新建表
    //df.write.mode("append").partitionBy("year").saveAsTable("")
    //2、向Spark创建的分区表写入数据
    // df.write.mode("append").partitionBy("year").saveAsTable("new_test_partition")
    //sql("insert into new_test_partition select * from temp_table")
    // df.write.insertInto("new_test_partition")


    //3、向在Hive里用Sql创建的分区表写入数据，抛出异常
    //    df.write.mode("append").partitionBy("year").saveAsTable("test_partition")

    // 4、解决方法
    //df.write.mode("append").format("Hive").partitionBy("datekey", "event").saveAsTable("default.czk_kudu2hive_test")

    //spark.sql("insert into default.czk_kudu2hive_test partition(datekey='',event) select * from temp_table")
    //spark.sql("insert into default.czk_hykb_test partition(`datekey`='20201024',`event`='score_change') select * from temp_table")
    //df.write.insertInto("default.czk_kudu2hive_test")
    //这样会抛出异常
    //    df.write.partitionBy("year").insertInto("test_partition")

    /*val spark = SparkSession.builder()
      .appName("kudu2hive" )
      .enableHiveSupport()
      .master("local")
      .getOrCreate()
    val df = spark.read.format("kudu")
      /*.options(Map("kudu.master" -> "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051", "kudu.table" -> "impala::test_db.test_table"))*/
      .options(Map("kudu.master" -> "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051", "kudu.table" -> "default.czk_cdc_myTable"))
      .load
    df.write.mode("append").format("Hive").partitionBy("create_time").saveAsTable("czk_kudu2hive_test")

    df.createOrReplaceTempView("tmp_table")
    val dataSchema: StructType = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)

    spark.sql("select * from tmp_table limit 10").show()*/
  }

}
