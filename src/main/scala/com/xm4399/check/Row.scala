package com.xm4399.check

import java.util

import com.xm4399.util.JDBCOnlineUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
 * @Auther: czk
 * @Date: 2020/8/25
 * @Description:
 */
object Row {

  def main(args: Array[String]): Unit = {

  }

  def tt( address : String, username : String, password : String, dbName : String, tableNmae : String, isSubTable :String ) : Unit ={
    val spark = SparkSession.builder().appName("GetKuduTableRowsCount").getOrCreate()
    val list = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, tableNmae, isSubTable)

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + tableNmae) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()
      .sample(false,0.001)

    println(">>>>>>>>>>>>>>>>>>>>" +jdbcDF.count())

    jdbcDF.map( item => {
      val
    })
    //Thread.sleep(20*60*1000)
    /*val kuduDF = spark.read
      .options(Map("kudu.master" -> "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051",
        "kudu.table" -> "czk_num_flase_false"))
      .format("kudu")
      .load
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>")
    println(kuduDF.count())*/

  }

}
