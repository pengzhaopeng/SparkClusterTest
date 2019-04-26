package cn.pengzhaopeng.spark.SparkRDD.topTearcher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求最受欢迎的老师
  */
object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    var sc = new SparkContext(conf)

    //指定从那里读数据
    val lines: RDD[String] = sc.textFile(args(0))
    //整理数据
    val tearcherAndOne: RDD[(String, Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val tearcher = line.substring(index + 1)
      (tearcher, 1)
    })
    //聚合
    val reduced: RDD[(String, Int)] = tearcherAndOne.reduceByKey(_ + _)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //触发Action执行计算|
    val result: Array[(String, Int)] = sorted.collect()

    //打印
    println(result.toBuffer)

    sc.stop()

  }
}
