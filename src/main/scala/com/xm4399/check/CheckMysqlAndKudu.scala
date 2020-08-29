package com.xm4399.check


import java.util

import com.xm4399.test.CollectTest
import com.xm4399.util.{CollectUtil, JDBCConfUtil, JDBCOnlineUtil, KuduUtil_2}
import org.apache.kudu.client.{KuduClient, KuduTable}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @Auther: czk
 * @Date: 2020/8/25
 * @Description:
 */
object CheckMysqlAndKudu {

  def checkBetweenMysqlAndKudu (jobID : String): Unit = {

    val confInfoArr = JDBCConfUtil.getConfInfoArr(jobID)
    val address = confInfoArr(0);
    val username = confInfoArr(1);
    val password = confInfoArr(2);
    val dbName = confInfoArr(3);
    val tableName = confInfoArr(4);
    val fields = confInfoArr(5)
    val isSubTable = confInfoArr(6);
    val kuduTableName = confInfoArr(8)
    if ("true".equals(isSubTable)){
      val spark = SparkSession.builder().appName("CheckData").getOrCreate()
      val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()
      val subTableList = new JDBCOnlineUtil().listAllSubTableName(address, username, password, dbName, tableName).asScala
      for ( oneSubTable <- subTableList){
        checkSubTable(spark, kuduClient, address, username, password, dbName, oneSubTable, kuduTableName, fields)
      }
      kuduClient.close()
      spark.close()
    } else {
      /*if ("false".equals(fields)){
        println("fields为 "  + fields)
        checkTable(address, username, password, dbName, tableName,  kuduTableName ,fields)
      } else {
        checkTableFields(address, username, password, dbName, tableName,  kuduTableName, fields)
      }*/
      checkTable(address, username, password, dbName, tableName,  kuduTableName ,fields)
    }

  }

  def checkSubTable(spark : SparkSession, kuduClient: KuduClient, address : String, username : String, password : String,
                    dbName : String, oneSubTableName : String,  kuduTableName : String, fields : String) : Unit ={
    val kuduTable = kuduClient.openTable(kuduTableName)

    var  map: util.LinkedHashMap[String, String] = null
    if ("false".equals(fields)){
      map = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, oneSubTableName)
      // println("map的长度 >>>>>>>>>>>>>>>>>>>>>>>>>>>" + map.size())
    } else {
      map = new JDBCOnlineUtil().getTableStruFields(address, username, password, dbName, oneSubTableName, fields)
      //println("map的长度 >>>>>>>>>>>>>>>>>>>>>>>>>>>" + map.size())
    }




    //val map = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, oneSubTableName)
    val allMysqlFieldsList = new CollectUtil().listAllMysqlFields(map).asScala
    var mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    mysqlPriKeyList.add("table_id")
    val kuduPriKeyList = new KuduUtil_2().listKuduPriKey(kuduTable)
    val collectUtil = new CollectUtil()

    if (! collectUtil.isSameForTwoLists(mysqlPriKeyList, kuduPriKeyList)){
      println("mysql表和kudu表主键不对应")
      //System.exit(1)
    }

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + oneSubTableName) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()
      .sample(false,0.01)

    println(">>>>>>>>>>>>>>>>>>>>" +jdbcDF.count())
    jdbcDF.show()
    // 分标的情况下,kudu表比mysql表多了个table_id列
    val table_id_str = oneSubTableName.substring(oneSubTableName.lastIndexOf("_") + 1, oneSubTableName.length)
    val table_id = table_id_str.toShort

    def circularProcess(item : Row) : Unit = {
      // 获取mysql一行所有字段和value的map集合
      var mysqlOneRowFieldsAndValue : Map[String, Any] = item.getValuesMap(allMysqlFieldsList)
      mysqlOneRowFieldsAndValue += ("table_id" -> table_id)
      var priKeyMap = Map[String, Any]()
      for ((field,value) <- mysqlOneRowFieldsAndValue){
        println("DF ROW的map>>> 字段 "+ field + "的值为 " + value )
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
          println("错>>>>>主键为 " + key + " 字段,值为 " +  value + " 的row")
        }
      }
    }

    jdbcDF.collect().foreach(circularProcess : Row => Unit)

  }


  def checkTable(address : String, username : String, password : String, dbName : String, tableName : String,
                 kuduTableName : String, fields : String) : Unit ={
    val spark = SparkSession.builder().appName("GetKuduTableRowsCount").getOrCreate()
    val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()
    val kuduTable = kuduClient.openTable(kuduTableName)

    var  map: util.LinkedHashMap[String, String] = null
    if ("false".equals(fields)){
      map = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, tableName)
     // println("map的长度 >>>>>>>>>>>>>>>>>>>>>>>>>>>" + map.size())
    } else {
      map = new JDBCOnlineUtil().getTableStruFields(address, username, password, dbName, tableName, fields)
      //println("map的长度 >>>>>>>>>>>>>>>>>>>>>>>>>>>" + map.size())
    }
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println("出方法的Map长度>>>>>>>>>"  + map.size())
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    //val map = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, tableName)
    val allMysqlFieldsList = new CollectUtil().listAllMysqlFields(map).asScala
    val mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    val kuduPriKeyList = new KuduUtil_2().listKuduPriKey(kuduTable)
    val collectUtil = new CollectUtil()

    if (! collectUtil.isSameForTwoLists(mysqlPriKeyList, kuduPriKeyList)){
      println("mysql表和kudu表主键不对应")
      //System.exit(1)
    }
    val jdbcDS = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + tableName) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()
      .sample(false,0.01)

    println(">>>>>>>>>>>>>>>>>>>>" +jdbcDS.count())
    jdbcDS.show()
    def circularProcess(item : Row ) : Unit = {
      // 获取mysql一行所有字段和value的map集合
      val mysqlOneRowFieldsAndValue : Map[String, Any] = item.getValuesMap(allMysqlFieldsList)
      // 分表的情况
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
          println("错>>>>>主键为 " + key + " 字段,值为 " +  value + " 的row")
        }
      }
    }
    jdbcDS.collect().foreach(circularProcess : Row => Unit)
    kuduClient.close()
    spark.close()
  }

  def checkTableFields(address : String, username : String, password : String, dbName : String, tableName : String,
                 kuduTableName : String, fields : String) : Unit ={
    val spark = SparkSession.builder().appName("GetKuduTableRowsCount").getOrCreate()
    val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()
    val kuduTable = kuduClient.openTable(kuduTableName)

    val map = new JDBCOnlineUtil().getTableStruFields(address, username, password, dbName, tableName, fields)
    /* if ("false".equals(fields)){
       map = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, tableName)
      // println("map的长度 >>>>>>>>>>>>>>>>>>>>>>>>>>>" + map.size())
     } else {
       map = new JDBCOnlineUtil().getTableStruFields(address, username, password, dbName, tableName, fields)
       //println("map的长度 >>>>>>>>>>>>>>>>>>>>>>>>>>>" + map.size())
     }*/
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println("出方法的Map长度>>>>>>>>>"  + map.size())
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    //val map = new JDBCOnlineUtil().getTableStru(address, username, password, dbName, tableName)
    val allMysqlFieldsList = new CollectUtil().listAllMysqlFields(map).asScala
    val mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    val kuduPriKeyList = new KuduUtil_2().listKuduPriKey(kuduTable)
    val collectUtil = new CollectUtil()

    if (! collectUtil.isSameForTwoLists(mysqlPriKeyList, kuduPriKeyList)){
      println("mysql表和kudu表主键不对应")
      //System.exit(1)
    }
    val jdbcDS = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + tableName) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()
      .sample(false,0.01)

    println(">>>>>>>>>>>>>>>>>>>>" +jdbcDS.count())
    jdbcDS.show()
    def circularProcess(item : Row ) : Unit = {
      // 获取mysql一行所有字段和value的map集合
      val mysqlOneRowFieldsAndValue : Map[String, Any] = item.getValuesMap(allMysqlFieldsList)
      // 分表的情况
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
          println("错>>>>>主键为 " + key + " 字段,值为 " +  value + " 的row")
        }
      }
    }
    jdbcDS.collect().foreach(circularProcess : Row => Unit)
    kuduClient.close()
    spark.close()
  }
}
