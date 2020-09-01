package com.xm4399.util

/**
 * @Auther: czk
 * @Date: 2020/9/1 
 * @Description:
 */
object OtherUtil2 {
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
