package cn.pengzhaopeng.spark.SparkRDD.topTearcher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求每个学科最受欢迎的老师
  * 2: 先按学科进行过滤，然后调用RDD的方法进行排序（多台机器+内存+磁盘），需要按任务提交多次
  */
object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    val topN = args(1).toInt

    val subjects = Array("bigdata", "javaee", "php")

    val conf = new SparkConf().setAppName("GroupFavTeacher2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据
    val lines: RDD[String] = sc.textFile(args(0))
    //整理数据
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_ + _)

    //遍历 并提交三次
    for (sb <- subjects) {
      //该RDD中对应的数据仅有一个学科的数据（因为过滤过了）
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)

      //现在调用的是RDD的sortBy方法，(take是一个action，会触发任务提交)
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(topN)

      //打印
      println(favTeacher.toBuffer)
    }

    sc.stop()
  }
}
