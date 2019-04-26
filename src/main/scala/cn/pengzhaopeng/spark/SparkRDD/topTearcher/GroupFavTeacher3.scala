package cn.pengzhaopeng.spark.SparkRDD.topTearcher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 求每个学科最受欢迎的老师
  * 自定义分区，然后在每个分区中进行排序（patitionBy, mapPartition）
  */
object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {
    val topN = args(1).toInt

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

//    reduced.cache()
    reduced.checkpoint()

    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    //自定义一个分区器 并且按照指定的分区器进行分区
    val sbPatitioner = new SubjectParitioner(subjects)

    //partitionBy按照指定的分区规则进行分区
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPatitioner)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })
    val r: Array[((String, String), Int)] = sorted.collect()
    println(r.toBuffer)

    sc.stop()

  }

  //自定义分区器
  class SubjectParitioner(sbs: Array[String]) extends Partitioner {

    //相当于主构造器（new的时候会执行一次）
    //用于存放规则的一个map
    val rules = new mutable.HashMap[String, Int]()
    var i = 0
    for (sb <- sbs) {
      rules.put(sb, i)
      i += 1
    }

    //返回分区的数量（下一个RDD有多少分区）
    override def numPartitions: Int = sbs.length

    //根据传入的key计算分区标号
    override def getPartition(key: Any): Int = {
      //获取学科名称
      val subject: String = key.asInstanceOf[(String, String)]._1
      //根据规则计算分区编号
      rules(subject)
    }
  }

}
