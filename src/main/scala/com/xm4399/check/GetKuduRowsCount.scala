package com.xm4399.check

import org.apache.kudu.client.{KuduClient, KuduPredicate}
import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/8/26 
 * @Description:
 */
object GetKuduRowsCount {

  def main(args: Array[String]): Unit = {
    getkuduRowsCount()
  }
  def getkuduRowsCount() :Unit ={
    // 创建kudu连接
    val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()

    // 设置表名
    val tableName = "default.czk_num_flase_false"

    // 获得表的连接
    val kuduTable = kuduClient.openTable(tableName)

    // 设置查询条件
    val dateline: Long = 1593510500L
    val schema = kuduTable.getSchema()
    val create_time = KuduPredicate.newComparisonPredicate(schema.getColumn("dateline"), KuduPredicate.ComparisonOp.LESS_EQUAL, dateline)
   /* val age = KuduPredicate.newComparisonPredicate(schema.getColumn("age"), KuduPredicate.ComparisonOp.LESS, 22)
    val city = KuduPredicate.newComparisonPredicate(schema.getColumn("city"), KuduPredicate.ComparisonOp.EQUAL, "beijing")*/

    // 执行查询操作
    val builder = kuduClient.newScannerBuilder(kuduTable)
      //.setProjectedColumnNames(List("create_time", "age", "city").asJava)
      .setProjectedColumnNames(List("id").asJava)
      .addPredicate(create_time)
      .build()
    var sum_count : Long = 0L
    while (builder.hasMoreRows()) {
      val results = builder.nextRows()
      val everyRowsCount = results.getNumRows
      sum_count = sum_count + everyRowsCount
    }
    println("kudu 表条件查询后的记录总数>>>> " + sum_count)
    // 关闭kudu连接
    builder.close()
    kuduClient.close()
  }

}
