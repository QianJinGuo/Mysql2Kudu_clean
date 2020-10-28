package com.xm4399.util2

import scalikejdbc._
object SetupJdbc {
  var driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://10.0.0.198:3306/kafka_metadata?useUnicode=true&characterEncoding=UTF8"
  val user = "linxingcai"
  val password = "linxingcai123"

  def apply(driver: String = driver, host: String = url, user: String = user, password: String = password): Unit = {

    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }


}


