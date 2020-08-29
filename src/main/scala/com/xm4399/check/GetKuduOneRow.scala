package com.xm4399.check


import com.xm4399.util.KuduUtil_2
import org.apache.kudu.client.{KuduClient, KuduPredicate, KuduTable}

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
 * @Auther: czk
 * @Date: 2020/8/27 
 * @Description:
 */
object GetKuduOneRow {

  def getkuduRowMap(kuduClient: KuduClient, kuduTable : KuduTable, map : Map[String, Any]) : Map[String, Any] ={
    // 创建kudu连接
    //val kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051").build()
    val kuduColList = new KuduUtil_2().listAllKuduCol(kuduTable)
    var scanner = kuduClient.newScannerBuilder(kuduTable)
      .setProjectedColumnNames(kuduColList)
    val schema = kuduTable.getSchema()
    // 遍历kudu表主键集合,进行添加条件查询
    for ((priKey, value) <- map){
      println("kudu scan 添加条件 >>PK为 " + priKey + "  值为 " +value)
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
          // 某个kuduRow单个字段和字段值
          val oneFieldAndValueTuple = new Tuple2(field,value)
          rowFieldAndValueMap += oneFieldAndValueTuple
        }
      }

    }
    // 关闭kudu连接
    builder.close()
    rowFieldAndValueMap
  }


}
