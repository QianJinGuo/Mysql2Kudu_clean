package com.xm4399.run

import com.xm4399.util.SampleCheck.checkSubTable
import com.xm4399.util.{ JDBCOnlineUtil, JDBCUtil, KuduUtil2, SampleCheck}

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
    val address = confInfoArr(0)
    val username = confInfoArr(1)
    val password = confInfoArr(2)
    val dbName = confInfoArr(3)
    val tableName = confInfoArr(4)
    val fields = confInfoArr(5)
    val isSubTable = confInfoArr(6)
    val kuduTableName = confInfoArr(7)
    val timestampFieldName = confInfoArr(8)
    //------------------------------------------------------
    val countResult = checkCount(jobID, address, username, password, dbName, tableName, isSubTable, kuduTableName, timestampFieldName, timestampStr)
    val sampleResult = SampleCheck.checkBetweenMysqlAndKudu(jobID)
    new JDBCUtil().insertCheckInfo(jobID, countResult, sampleResult )
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
      val errorMsg = "isSubTable参数有误";
      new JDBCUtil().insertErrorInfo(jobID, "CheckData", errorMsg);
    }
    // kudu表记录数
    val kuduCount = KuduUtil2.getkuduRowsCount(kuduTableName, timestampFieldName, timestampStr)
    if (mysqlCount == kuduCount){
      true
    } else {
      val errorMsg = "mysql记录数为 " + mysqlCount + ",  kudu记录数为 "  + kuduCount;
      new JDBCUtil().insertErrorInfo(jobID, "CheckData", errorMsg);
      false
    }
  }


}
