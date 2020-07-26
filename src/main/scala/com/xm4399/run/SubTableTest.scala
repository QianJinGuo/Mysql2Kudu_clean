package com.xm4399.run

import com.xm4399.test.MyTsetCreate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


import org.apache.spark.sql._
object SubTableTest {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
    // 读取MySQL数据
   /* val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "chenzhikun.first_canal") //dbName.tableName
      .option("user", "canal")
      .option("password", "canal")
      .load()


      jdbcDF.withColumn("table_name",lit("table_1"))
    println("增加一列之后>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")*/
    println("selectExpr之前>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val jdbcDF2 = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "chenzhikun.first_canal") //dbName.tableName
      .option("user", "canal")
      .option("password", "canal")
      .load()
      .withColumn("table_id",lit("table1")) //分表的情况下,kudu表相比于mysql分表多了
     // .show()
    val jdbcDF6 = jdbcDF2.show()
    //val arr : Array[String] = ("id","name","")
    var array = Array("table_id", "id", "name")
    //val jdbcDF3 = jdbcDF2.selectExpr(array:_*)
    println("selectExpr之后>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val jdbcDF3 = jdbcDF2.selectExpr(array:_*).show
    //val jdbcDF4 = jdbcDF2.select("")
    //val df_table_id =jdbcDF2.select("table_id","id","name")

    //println("调整的列")
    //df_table_id.show()
    //println("删除列后<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //jdbcDF2.drop("table_id").show

   // println("添加列后<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //jdbcDF2.intersect(df_table_id).show()






    //jdbcDF2: org.apache.spark.sql.DataFrame = [id: bigint]


    }


  }
