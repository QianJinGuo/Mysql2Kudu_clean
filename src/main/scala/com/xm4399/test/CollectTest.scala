package com.xm4399.test

/**
 * @Auther: czk
 * @Date: 2020/8/31 
 * @Description:
 */
object CollectTest {
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
