package cn.pengzhaopeng.spark.SparkRDD.ipLocation

import scala.io.{BufferedSource, Source}

object MyUtils {

  /**
    * ip转换成十进制
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 查找指定ip在ip数组中的下标
    */
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if (ip >= lines(middle)._1 && ip <= lines(middle)._2)
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  /**
    * 读取ip规则
    */
  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //整理ip 并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def main(args: Array[String]): Unit = {
    //数据在内存中
    val rules: Array[(Long, Long, String)] = readRules("E:\\bigDataTest\\ip.txt")
    //将ip地址转换成十进制
    val ipNum: Long = ip2Long("47.52.58.222")
    //查找
    val index: Int = binarySearch(rules, ipNum)
    val tp: (Long, Long, String) = rules(index)
    val province: String = tp._3
    println(province)
  }
}
