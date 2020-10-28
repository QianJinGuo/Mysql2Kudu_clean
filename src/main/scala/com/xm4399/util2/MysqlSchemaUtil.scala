package com.xm4399.util2

import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import scalikejdbc._
object MysqlSchemaUtil {

  def getMysqlSchema(topic: String): StructType = {
    var schema = StructType(List())
    val driver = "com.mysql.jdbc.Driver"
    val username = "analysis"
    val password = "CeEgp24234"
    val url = "jdbc:mysql://10.0.0.246:3306/gprp_event_analysis?useUnicode=true&characterEncoding=UTF8"
    //    val username = "gprp"
    //    val password = "gprp@@4399"
    //    val url = "jdbc:mysql://10.0.0.211:3307/event_analysis?useSSL=false&useUnicode=true&characterEncoding=UTF8"

    try {
      SetupJdbc(driver, url, username, password)
      implicit val session = AutoSession
      val filedList = new ListBuffer[StructField]()
      val querySql = "select topic, columnname, datatype, is_null from hivetopic_attribute_message where is_synchronous in (1, 3) and topic='%s' order by colid asc, id asc ".format("hykb_event")
     //val querySql = "select topic, columnname, datatype, is_null from hivetopic_attribute_message where is_synchronous in (1, 3) and topic='czk_hykb_test' order by colid asc, id asc "
      val theset = MysqlUtil.getListResult(querySql)
      val DecimalType = DataTypes.createDecimalType(15, 4)
      val fieldSet = scala.collection.mutable.Set[String]()
      val dataTypeMap: Map[String, DataType] = Map("string" -> StringType, "int" -> IntegerType, "long" -> LongType, "bigint" -> LongType, "boolean" -> BooleanType, "decimal" -> DecimalType)
      theset.map(r => {
        val topic = r.get("topic").get.toString
        val columnname = r.get("columnname").get.toString.toLowerCase()
        val datatype = r.get("datatype").get.toString
        if (!(fieldSet contains columnname)) {
          fieldSet += columnname
          filedList += StructField(columnname, dataTypeMap.get(datatype).get, nullable = true)
        }
      })
      filedList += StructField("datekey", StringType, nullable = true)
      filedList += StructField("event", StringType, nullable = true)
      schema = StructType(filedList)
      println(schema.treeString)

    } catch {
      case e: Throwable => e.printStackTrace
    }
    return schema;

  }

  def main(args: Array[String]): Unit = {
    println("**********")
    val s = getMysqlSchema("hykb_event")
    print(s.treeString)
  }

}
