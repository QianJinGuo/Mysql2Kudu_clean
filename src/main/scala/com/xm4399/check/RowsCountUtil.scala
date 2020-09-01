package com.xm4399.check

import org.apache.kudu.client.{KuduClient, KuduPredicate}
import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/8/26 
 * @Description:
 */
object RowsCountUtil {

  def main(args: Array[String]): Unit = {
    //getkuduRowsCount()
  }
  def getkuduRowsCount(kuduTableName : String, timestampFieldName : String, timestampNum : String) :Long ={
    val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()
    val kuduTable = kuduClient.openTable(kuduTableName)
    // 设置查询条件
    val timestampLong: Long = timestampNum.toLong
    val schema = kuduTable.getSchema()
    val timestampLimit = KuduPredicate.newComparisonPredicate(schema.getColumn(timestampFieldName), KuduPredicate.ComparisonOp.LESS_EQUAL, timestampLong)

    // 执行查询操作
    val builder = kuduClient.newScannerBuilder(kuduTable)
      //.setProjectedColumnNames(List("create_time", "age", "city").asJava)
      //.setProjectedColumnNames(List("id").asJava)
      .addPredicate(timestampLimit)
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
    sum_count
  }

}
