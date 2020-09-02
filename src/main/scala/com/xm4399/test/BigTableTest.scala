package com.xm4399.test

import java.util
import java.util.Properties

import com.xm4399
import com.xm4399.util.{CollectUtil, JDBCOnlineUtil, JDBCUtil, OtherUtil2}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import scala.collection.mutable
/**
 * @Auther: czk
 * @Date: 2020/9/2 
 * @Description:
 */
object BigTableTest {

  def main(args: Array[String]): Unit = {
    readSomePartition()
  }
  def readSomePartition(): Unit = {
    //val spark = SparkSession.builder().appName("MysqlFullPullKudu").getOrCreate()
    val spark = SparkSession.builder().appName("MysqlFullPullKudu").getOrCreate()
    // 获取mysql主键
    val map = new JDBCOnlineUtil().getTablePriKeyStru("10.0.0.92:3310","cnbbsReadonly","LLKFN*k241235","4399_cnbbs","forums_user_reply_1")
    val mysqlPriKeyList = new CollectUtil().listMysqlPriKey(map)
    val pkStr = mysqlPriKeyList.get(0)

    val partitons = new Array[String](3);
    //三个分区,第一个为1-500w,第二为500w-1000w,第三为1000为2000w.多出的50为缓解拉取过程中的增量数据
    partitons(0) = s"1=1 and $pkStr >=(select $pkStr from forums_user_reply_1 order by $pkStr limit 1) limit  5000000"
    partitons(1) = s"1=1 and $pkStr >=(select $pkStr from forums_user_reply_1 order by $pkStr limit 4999950,1) limit  5000100"
    partitons(2) = s"1=1 and $pkStr >=(select $pkStr from forums_user_reply_1 order by $pkStr limit 9999950,1) limit  0,10000000"

    val prop = new Properties()

    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "cnbbsReadonly")
    prop.setProperty("password" , "LLKFN*k241235" )
    var jdbcDF = spark.read.jdbc("jdbc:mysql://10.0.0.92:3310/4399_cnbbs", "forums_user_reply_1", partitons, prop)

    import spark.implicits._

    val longTextFiledsList = new JDBCOnlineUtil().listLongTextFields("10.0.0.92:3310","cnbbsReadonly","LLKFN*k241235","4399_cnbbs","forums_user_reply_1")
    if (longTextFiledsList.size() !=0){
      val list = longTextFiledsList.asScala
      jdbcDF.map(item => subStringColumn(item,list))
    }

    //jdbcDF.
    jdbcDF.withColumn("name",regexp_replace(col("name"),".*",".{0,400}"))
    jdbcDF.rdd.map(x => {

      x.getAs("")
      var res = x.get(0).toString()
      if(x.get(1).toString().length >22) {
        res = res.substring(1)
      }
      val value: Any = x.get(0)
      (x.get(0).toString.toLong, res, x.get(2).toString)
    }).toDF("aa","bb", "cc")

    val KUDU_MASTERS = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051"
    jdbcDF
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "default.czk_limit_test")
      .save()
    spark.close()
    //new JDBCUtil().updateJobState(jobID, "Run_RealTime");
  }

  def subStringColumn(item : Row, list: mutable.Buffer[String]  ) : Row = {
    val longTextFieldAndValue : Map[String, Any] = item.getValuesMap(list)
    for ((field,value) <- longTextFieldAndValue){}

    item
  }

}
