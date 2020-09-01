package com.xm4399.run

import com.xm4399.check.CheckMysqlAndKudu.checkSubTable
import com.xm4399.check.{CheckMysqlAndKudu, KuduUtil}
import com.xm4399.util.{JDBCUtil, JDBCErrorLogAndCheckUtil, JDBCOnlineUtil}

import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/8/29
 * @Description:
 */
object RunDataCheck {
  def main(args: Array[String]): Unit = {
    val jobID= args(0)
    val timestampStr = args(1)
    val confInfoArr = new JDBCUtil().getConfInfoArr(jobID)
    val address = confInfoArr(0);
    val username = confInfoArr(1);
    val password = confInfoArr(2);
    val dbName = confInfoArr(3);
    val tableName = confInfoArr(4);
    val fields = confInfoArr(5)
    val isSubTable = confInfoArr(6);
    val kuduTableName = confInfoArr(8)
    val timestampFieldName = confInfoArr(10)
    //------------------------------------------------------
    val countResult = checkCount(jobID, address, username, password, dbName, tableName, isSubTable, kuduTableName, timestampFieldName, timestampStr)
    println("记录数检查结果为 " + countResult)
    val sampleResult = CheckMysqlAndKudu.checkBetweenMysqlAndKudu(jobID)
    println("抽样检查结果为 " + sampleResult)
    new JDBCErrorLogAndCheckUtil().insertCheckInfo(jobID, countResult, sampleResult )
  }



  def checkCount(jobID : String, address : String, username : String, password : String, dbName : String, tableName : String,
                 isSubTable : String, kuduTableName : String, timestampFieldName : String, timestampStr : String ): Boolean ={
    var mysqlCount : Long = 0L
    // 获取mysql表记录数
    if ("false".equals(isSubTable)){
      mysqlCount = new JDBCOnlineUtil().getTableCount(address, username,password, dbName, tableName, timestampFieldName,timestampStr)
    } else if ("true".equals(isSubTable)){
      val subTableList = new JDBCOnlineUtil().listAllSubTableName(address, username, password, dbName, tableName).asScala
      for ( oneSubTable <- subTableList){
        val oneTableCount = new JDBCOnlineUtil().getTableCount(address, username,password, dbName, oneSubTable, timestampFieldName,timestampStr)
        mysqlCount = oneTableCount + mysqlCount
      }
    } else {
      println("isSubTable参数有误")
    }
    // kudu表记录数
    val kuduCount = KuduUtil.getkuduRowsCount(kuduTableName, timestampFieldName, timestampStr)
    if (mysqlCount == kuduCount){
      println("mysql和kudu记录数相等")
      true
    } else {
      println("mysql: "+mysqlCount)
      println("kudu: "+ kuduCount)
      println("mysql和kudu记录数不相等")
      val errorMsg = "mysql记录数为 " + mysqlCount + ",  kudu记录数为 "  + kuduCount;
      new JDBCErrorLogAndCheckUtil().insertErroeInfo(jobID, "CheckData", errorMsg);
      false
    }
  }


}
