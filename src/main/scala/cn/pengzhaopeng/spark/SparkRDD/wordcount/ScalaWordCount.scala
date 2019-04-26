package cn.pengzhaopeng.spark.SparkRDD.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Sorting

/**
  * spark 单词统计
  */
object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    //创建spark配置 设置应用程序名字
    val conf = new SparkConf().setAppName("ScalaWordCount")
    //创建spark执行入口
    val sc = new SparkContext(conf)
    //指定从哪里读取创建RDD(弹性分布式数据库)
    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词组合在一起
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}
