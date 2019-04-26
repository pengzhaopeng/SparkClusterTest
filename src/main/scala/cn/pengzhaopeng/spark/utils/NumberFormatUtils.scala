package cn.pengzhaopeng.spark.utils

import scala.util.Try

/**
  * 各种字串 数字 等转换
  */
object NumberFormatUtils {

  /**
    * 判断是否是你预期的类型
    *
    * @param str   输入的字符串
    * @param dtype 期待的类型
    */
  def checkIsType(str: String, dtype: String): Boolean = {

    var c: Try[Any] = null
    if ("double".equals(dtype)) {
      c = Try(str.toDouble)
    } else if ("int".equals(dtype)) {
      c = Try(str.toInt)
    } else if ("float".equals(dtype)) {
      c = Try(str.toFloat)
    }
    val result: Boolean = c match {
      case scala.util.Success(_) => true
      case _ => false
    }
    result
  }
}
