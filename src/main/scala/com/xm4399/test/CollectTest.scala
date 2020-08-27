package com.xm4399.test

import org.apache.spark.sql.SparkSession

/**
 * @Auther: czk
 * @Date: 2020/8/26 
 * @Description:
 */
object CollectTest {
  def main(args: Array[String]): Unit = {
    val map1 = Map("1" -> 1, "2" -> "2", "3" -> 1.4 ,"4" -> 1L)
    val map2 = Map("1" -> 1, "2" -> "2", "3" -> 1.4 ,"4" -> 1L)
     print(isSameFromTwoMap(map1, map2))

  }

  def isSameFromTwoMap (map1: Map[String,Any], map2: Map[String,Any]) : Boolean={
    if (map1.size != map2.size){
      return false
    }
    for (key <- map1.keys){
      if (!map2.contains(key)){
        return  false
      } else {
        if (!map2.get(key).equals(map1.get(key))){
          return  false
        }
      }
    }
    true
  }

}
