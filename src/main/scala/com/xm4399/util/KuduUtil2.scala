package com.xm4399.util

import org.apache.kudu.client.{KuduClient, KuduPredicate, KuduTable}
import scala.collection.JavaConverters._
/**
 * @Auther: czk
 * @Date: 2020/8/27
 * @Description:
 */
object KuduUtil2 {
  // 获取kudu每个row的字段和对应的值
  def getkuduRowMap(kuduClient: KuduClient, kuduTable : KuduTable, priKeyMap : Map[String, Any]) : Map[String, Any] ={
    val kuduColList = new KuduUtil().listAllKuduCol(kuduTable)
    var scanner = kuduClient.newScannerBuilder(kuduTable)
      .setProjectedColumnNames(kuduColList)
    val schema = kuduTable.getSchema()

    // 遍历kudu表主键集合,进行添加条件查询
    for ((priKey, value) <- priKeyMap){
      scanner.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(priKey), KuduPredicate.ComparisonOp.EQUAL, value))//
    }
    val builder = scanner.build()
    var rowFieldAndValueMap = Map[String, Any]()
    while (builder.hasMoreRows()) {
      val results = builder.nextRows()
      while (results.hasNext){
        val result  = results.next()
        val scalaKuduColList = kuduColList.asScala
        for (field <- scalaKuduColList){
          val value = result.getObject(field)
          val oneFieldAndValueTuple = new Tuple2(field,value)
          rowFieldAndValueMap += oneFieldAndValueTuple
        }
      }

    }
    // 关闭kudu连接
    builder.close()
    rowFieldAndValueMap
  }

  // 获取kudu的记录数
  def getkuduRowsCount(kuduTableName : String, timestampFieldName : String, timestampNum : String) :Long ={
    val kuduClient = new KuduClient.KuduClientBuilder(new ConfUtil().getValue("kuduMaster")	).build()
    val kuduTable = kuduClient.openTable(kuduTableName)
    // 设置查询条件
    val timestampLong: Long = timestampNum.toLong
    val schema = kuduTable.getSchema()
    val create_time = KuduPredicate.newComparisonPredicate(schema.getColumn(timestampFieldName), KuduPredicate.ComparisonOp.LESS_EQUAL, timestampLong)


    // 执行查询操作
    val builder = kuduClient.newScannerBuilder(kuduTable)
      //.setProjectedColumnNames(List("create_time", "age", "city").asJava)
     // .setProjectedColumnNames(List("id").asJava)
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
    sum_count
  }


}
