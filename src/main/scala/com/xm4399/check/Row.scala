package com.xm4399.check


import com.xm4399.test.CollectTest
import com.xm4399.util.{CollectUtil, JDBCOnlineUtil, KuduUtil_2}
import org.apache.kudu.client.KuduClient
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * @Auther: czk
 * @Date: 2020/8/25
 * @Description:
 */
object Row {

  def main(args: Array[String]): Unit = {
    tt("10.0.0.92:3310", "cnbbsReadonly","LLKFN*k241235","4399_cnbbs",
      "thread_image_like_user_0", "false", "default.czk_num_flase_false")
  }

  def tt( address : String, username : String, password : String, dbName : String, tableNmae : String, isSubTable :String,kuduTableName : String) : Unit ={
    val spark = SparkSession.builder().appName("GetKuduTableRowsCount").getOrCreate()
    val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()
    val kuduTable = kuduClient.openTable(kuduTableName)

    val list = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, tableNmae, isSubTable)
    val allMysqlFieldsList = new CollectUtil().listAllMysqlFields(list).asScala
    val mysqlPriKeyList = new CollectUtil().listMysqlPriKey(list)
    val kuduPriKeyList = new KuduUtil_2().listKuduPriKey(kuduTable)
    val collectUtil = new CollectUtil()
    if ( ! collectUtil.isSameForTwoLists(mysqlPriKeyList, kuduPriKeyList)){
      println("mysql表和kudu表主键不对应")
      //System.exit(1)
    }
    val jdbcDS = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + tableNmae) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()
      .sample(false,0.1)


    println(">>>>>>>>>>>>>>>>>>>>" +jdbcDS.count())
    jdbcDS.show()

    //import  spark.implicits._
    def circularProcess(item : Row) : Unit = {

      println("开始遍历>>>")
      // 获取mysql一行所有字段和value的map集合
      val mysqlOneRowFieldsAndValue : Map[String, Any] = item.getValuesMap(allMysqlFieldsList)
      var priKeyMap = Map[String, Any]()
      for ((field,value) <- mysqlOneRowFieldsAndValue){
        if (mysqlPriKeyList.contains(field)){
          println("mysql主键字段 "+ field +" 的值为 " + value )
          priKeyMap += (field -> value)
        }
      }
     // 获取kudu one row 所有字段和value的map集合
     val kuduOneRowFieldsAndValue = GetKuduOneRow.getkuduRowMap(kuduClient, kuduTable, priKeyMap)
      val isSame = CollectTest.isSameFromTwoMap(mysqlOneRowFieldsAndValue, kuduOneRowFieldsAndValue)
      if (isSame){
        println ("该行相同")
      } else{
        for ((key,value) <- priKeyMap){
          println("主键为 " + key + " 的row值不对")
        }
      }
    }

    jdbcDS.collect().foreach(circularProcess : Row => Unit)

    kuduClient.close()
  }



}
