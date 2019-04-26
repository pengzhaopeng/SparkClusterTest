package cn.pengzhaopeng.spark.SparkRDD.topTearcher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 求每个学科最受欢迎的老师
  * 在聚合时就用自定义分区，可以减少shuffle
  */
object GroupFavTeacher4 {
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


    //计算有多少学科
    val subjects: Array[String] = sbjectTeacherAndOne.map(_._1._1).distinct().collect()

    //自定义一个分区器，并且按照指定的分区器进行分区
    val sbPatitioner = new SubjectParitioner2(subjects)

    //聚合，聚合是就按照指定的分区器进行分区
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(sbPatitioner, _ + _)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val list3 = new ListBuffer[((String, String), Int)]
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      //      it.toList.sortBy(_._2).reverse.take(topN).iterator
      list3.append(it.next())
      val list4: ListBuffer[((String, String), Int)] = list3.sortWith((a, b) => a._2 > b._2)
      if (list4.length > 2) {
        list4.remove(0)
      }
      list4.iterator
    })
    //    sorted.saveAsTextFile("E:\\out")
    println(list3)

    sc.stop()
  }
}

//自定义分区器
class SubjectParitioner2(sbs: Array[String]) extends Partitioner {

  //相当于主构造器（new的时候回执行一次）
  //用于存放规则的一个map
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <- sbs) {
    //rules(sb) = i
    rules.put(sb, i)
    i += 1
  }

  //返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = sbs.length

  //根据传入的key计算分区标号
  //key是一个元组（String， String）
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算分区编号
    rules(subject)
  }
}
