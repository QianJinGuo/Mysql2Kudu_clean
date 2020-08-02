package com.xm4399.run

/**
 * @Auther: czk
 * @Date: 2020/8/2 
 * @Description:
 */
object test {

  def main(args: Array[String]): Unit = {
    val oneSubTableName = "thread_image_like_user_7"
    val table_id = oneSubTableName.substring(oneSubTableName.lastIndexOf("_") + 1, oneSubTableName.length)
    val id = table_id.toInt
    print(id)
  }

}
