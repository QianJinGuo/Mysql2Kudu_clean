package com.xm4399.util

import java.util

import com.xm4399
import org.apache.kudu.client.KuduClient
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/8/25
 * @Description:
 */
object SampleCheck {

  def checkBetweenMysqlAndKudu (jobID : String): Boolean = {
    val confInfoArr = new JDBCUtil().getConfInfoArr(jobID)
    val address = confInfoArr(0);
    val username = confInfoArr(1);
    val password = confInfoArr(2);
    val dbName = confInfoArr(3);
    val tableName = confInfoArr(4);
    val fields = confInfoArr(5)
    val isSubTable = confInfoArr(6);
    val kuduTableName = confInfoArr(8)
    // -------------------------------------------------------------------------
    val spark = SparkSession.builder().appName("CheckData").getOrCreate()
    val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()
    try{
      if ("true".equals(isSubTable)){
        val subTableList = new JDBCOnlineUtil().listAllSubTableName( address, username, password, dbName, tableName).asScala
        for ( oneSubTable <- subTableList){
          val result = checkSubTable(jobID, spark, kuduClient, address, username, password, dbName, oneSubTable, kuduTableName, fields)
          if (result.equals(false)){
            return false
          }
        }
        true
      } else {
        checkTable(jobID, spark, kuduClient, address, username, password, dbName, tableName,  kuduTableName ,fields)
      }
    } catch {
      case e : Exception => {
        val errorMsg = new OtherUtil().getException(e)
        new JDBCUtil().insertErroeInfo(jobID, "CheckData", errorMsg )
        false
      }
    } finally{
      kuduClient.close()
      spark.close()
    }
  }

  def checkSubTable(jobID : String, spark : SparkSession, kuduClient: KuduClient, address : String, username : String, password : String,
                    dbName : String, oneSubTableName : String,  kuduTableName : String, fields : String) : Boolean ={
    val kuduTable = kuduClient.openTable(kuduTableName)
    var  map: util.LinkedHashMap[String, String] = null
    // 分为全表拉取和安字段拉取两种
    if ("false".equals(fields)){
      map = new JDBCOnlineUtil().getTablePriKeyStru(address, username, password, dbName, oneSubTableName)
    } else {
      map = new JDBCOnlineUtil().getTableStruFields(address, username, password, dbName, oneSubTableName, fields)
    }
    val allMysqlFieldsList = new CollectUtil().listAllMysqlFields(map).asScala
    var mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    mysqlPriKeyList.add("table_id")
    val kuduPriKeyList = new KuduUtil().listKuduPriKey(kuduTable)
    val collectUtil = new CollectUtil()
    if (! collectUtil.isSameForTwoLists(mysqlPriKeyList, kuduPriKeyList)){
      println("mysql表和kudu表主键不对应")
      new JDBCUtil().insertErroeInfo(jobID, "CheckData", "mysql表和kudu表主键不对应")
      return false
    }
    val jdbcDS = spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + address)
      .option("dbtable", dbName + "." + oneSubTableName) //dbName.tableName
      .option("user", username)
      .option("password", password)
      .load()
      .sample(false,0.01)

    // 分标的情况下,kudu表比mysql表多了个table_id列
    val table_id_str = oneSubTableName.substring(oneSubTableName.lastIndexOf("_") + 1, oneSubTableName.length)
    val table_id = table_id_str.toShort

    def circularProcess(item : Row) : Boolean = {
      // 获取mysql一行所有字段和value的map集合
      var mysqlOneRowFieldsAndValue : Map[String, Any] = item.getValuesMap(allMysqlFieldsList)
      mysqlOneRowFieldsAndValue += ("table_id" -> table_id)
      var priKeyMap = Map[String, Any]()
      for ((field,value) <- mysqlOneRowFieldsAndValue){
        if (mysqlPriKeyList.contains(field)){
          priKeyMap += (field -> value)
        }
      }

      // 获取kudu one row 所有字段和value的map集合
      val kuduOneRowFieldsAndValue = xm4399.util.KuduUtil2.getkuduRowMap(kuduClient, kuduTable, priKeyMap)
      val isSame = OtherUtil2.isSameFromTwoMap(mysqlOneRowFieldsAndValue, kuduOneRowFieldsAndValue)
      if (!isSame){
        for ((key,value) <- priKeyMap){
          val errorMsg = "主键为 " + key + " 字段,值为 " +  value + " 的row不对应"
          new JDBCUtil().insertErroeInfo(jobID, "CheckData", errorMsg)
        }
        return false
      }
      true
    }
    val rowResultArr = jdbcDS.collect().map(item => circularProcess(item))
    for (result <- rowResultArr){
      if (!result){
        return false
      }
    }
    true
  }


  def checkTable(jobID : String, spark : SparkSession, kuduClient: KuduClient,address : String, username : String,
                 password : String, dbName : String, tableName : String, kuduTableName : String, fields : String) : Boolean ={
    val kuduTable = kuduClient.openTable(kuduTableName)
    var  map: util.LinkedHashMap[String, String] = null
    if ("false".equals(fields)){
      map = new JDBCOnlineUtil().getTablePriKeyStru(address, username, password, dbName, tableName)
    } else {
      map = new JDBCOnlineUtil().getTableStruFields(address, username, password, dbName, tableName, fields)
    }
    val allMysqlFieldsList = new CollectUtil().listAllMysqlFields(map).asScala
    val mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    val kuduPriKeyList = new KuduUtil().listKuduPriKey(kuduTable)
    val collectUtil = new CollectUtil()
    if (! collectUtil.isSameForTwoLists(mysqlPriKeyList, kuduPriKeyList)){
      println("mysql表和kudu表主键不对应")
      new JDBCUtil().insertErroeInfo(jobID, "CheckData", "mysql表和kudu表主键不对应")
      return false
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
    //对比mysql row和kudu row
    def circularProcess(item : Row ) : Boolean = {
      // 获取mysql一行所有字段和value的map集合
      val mysqlOneRowFieldsAndValue : Map[String, Any] = item.getValuesMap(allMysqlFieldsList)
      // 分表的情况
      var priKeyMap = Map[String, Any]()
      for ((field,value) <- mysqlOneRowFieldsAndValue){
        if (mysqlPriKeyList.contains(field)){
          priKeyMap += (field -> value)
        }
      }
      // 获取kudu one row 所有字段和value的map集合
      val kuduOneRowFieldsAndValue = xm4399.util.KuduUtil2.getkuduRowMap(kuduClient, kuduTable, priKeyMap)
      val isSame = OtherUtil2.isSameFromTwoMap(mysqlOneRowFieldsAndValue, kuduOneRowFieldsAndValue)
      if (!isSame){
        for ((key,value) <- priKeyMap){
          val errorMsg = "主键为 " + key + " 字段,值为 " +  value + " 的row不对应"
          new JDBCUtil().insertErroeInfo(jobID, "CheckData", errorMsg)
        }
        return false
      }
      true
    }
    //jdbcDS.collect().map(circularProcess : Row => Unit)
    val rowResultArr = jdbcDS.collect().map(item => circularProcess(item))
    for (result <- rowResultArr){
      if (!result){
        return false
      }
    }
    true
  }

}
