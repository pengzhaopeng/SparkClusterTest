package cn.pengzhaopeng.spark.SparkRDD.topTearcher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 求每个学科最受欢迎的老师
  * 1：聚合后按学科进行分组，然后在每个分组中进行排序（调用的是Scala集合的排序）
  */
object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {

    val topN = args(1).toInt
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    //整理数据
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val httpHost: String = line.substring(0, index)
      val subject: String = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_ + _)
    //分组排序
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    //经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(topN))
    //收集结果
    val r: Array[(String, List[((String, String), Int)])] = sorted.collect()
    //打印
    println(r.toBuffer)

    sc.stop()

  }
}
