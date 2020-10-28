package com.xm4399.util2

import java.util.Properties


import scalikejdbc.{DB, SQL}

/**
  * Created by readme on 17-5-31.
  */


case class LogTrace(hiveTable: String, Logtrace: String, datekey: String)


object MysqlUtil {

  //  val config = ConfigService.getConfig("centerOfUsers.log_monitor_mysql")
  val driver = "com.mysql.jdbc.Driver"
  val user = "linxingcai"
  val password = "linxingcai123"
  val url = "jdbc:mysql://10.0.0.198:3306/test?useUnicode=true&characterEncoding=UTF8"
  var prop = new Properties()
  prop.put("user", user)
  prop.put("password", password)
  prop.put("driver", driver)
  prop.put("url", url)
  val logTraceTable = "hive_table_logtrace"
  val logTraceCol = Array("hive_table", "logtrace", "datekey")
  val hdfToHiveTable = "hdfs_to_hive_logs"
  //  val topicThriftTable = "topic_thrift"
  //  val topicThriftTableCols = Array("source_type", "name", "datekey", "minutes_num", "field_name", "stat_kind", "value", "timestamp", "field_type")

  /**
    * 插入trace
    *
    * @param seq
    */
  def inSertLogTraceTable(seq: Seq[Seq[Any]]): Unit = {
    DB.localTx(implicit session => {
      SQL(
        s"""insert into $logTraceTable (hive_table, logtrace, datekey)
           | value (?,?,?) """.stripMargin).batch(seq: _*).apply()
    }
    )
  }

  /**
    * 清空当天的trace
    *
    * @param datekey
    * @param table
    */
  def cleanLogTraceTable(datekey: String, table: String): Unit = {
    DB.localTx(implicit session => {
      try {

        val sqlStr = s"delete from ${logTraceTable} where datekey = '${datekey}' and hive_table='$table'"
        println(sqlStr)
        SQL(sqlStr).update.apply()
      }
      catch {
        case e: Throwable => e.printStackTrace()
      }
    }
    )
  }


  /**
    *
    * @param database
    * @param table
    * @param datekey
    * @param toStr
    * @return
    */
  def getHiveLastTimeKey(database: String, table: String, datekey: String, toStr: String): String = {
    val timeKey: Option[String] = DB readOnly { implicit session =>
      SQL(s"select max(lasttime_str) as timekey from $hdfToHiveTable where datekey='$datekey' and databasename='$database' and tablename='$table'").map(rs => rs.string("timekey")).single.apply()
    }
    val fromDefault = datekey + "-00:00"
    timeKey.getOrElse(fromDefault).toString
  }

  /**
    * 返回查询结果
    * @param sql
    * @return
    */
  def getListResult(sql: String): List[Map[String, Any]] = {
    DB readOnly {implicit session =>
      SQL(sql).map(_.toMap()).list.apply()
    }
  }

  /**
    * 插入trace
    *
    * @param seq
    */
  def inSertHiveLastTimeKey(seq: Seq[Seq[Any]]): Unit = {
    DB.localTx(implicit session => {
      SQL(
        s"""insert into $hdfToHiveTable (databasename, tablename, lasttime_str,success_time,datekey)
           | value (?,?,?,?,?) """.stripMargin).batch(seq: _*).apply()
    }
    )
  }
}
