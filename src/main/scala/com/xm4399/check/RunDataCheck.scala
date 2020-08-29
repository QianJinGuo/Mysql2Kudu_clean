package com.xm4399.check

import com.xm4399.util.{JDBCConfUtil, JDBCOnlineUtil}

/**
 * @Auther: czk
 * @Date: 2020/8/29 
 * @Description:
 */
object RunDataCheck {
  def main(args: Array[String]): Unit = {
    val jobID= args(0)
    val confInfoArr = JDBCConfUtil.getConfInfoArr(jobID)
    val address = confInfoArr(0);
    val username = confInfoArr(1);
    val password = confInfoArr(2);
    val dbName = confInfoArr(3);
    val tableName = confInfoArr(4);
    val fields = confInfoArr(5)
    val isSubTable = confInfoArr(6);
    val kuduTableName = confInfoArr(8)
    val timestampFieldName = confInfoArr(10)
    new JDBCOnlineUtil().getTableCount(address, username,password, dbName, tableName, timestampFieldName,-1L)
    val mysqlCount =
  }

}
