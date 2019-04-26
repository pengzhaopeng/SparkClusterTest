package cn.pengzhaopeng.spark.SparkRDD.ipLocation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据访问日志的ip地址计算出访问者的归属地，并且按省份，计算出访问次数，然后将计算好的结果写入到MySQL
  * 1:整理数据，切分出ip字段，然后将ip地址转换成十进制
  * 2:加载规则，整理规则，取出有用的字段，然后将数据缓存到内存中(Executor中的内存中)
  * 3:将访问log与ip个则进行匹配（二分查找）
  * 4:取出对应的省份名称，然后将其和1 组合在一起
  * 5:按省份名进行聚合
  * 6:将聚合后的数据写入到MySQL中
  */
object IpLoaction1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IpLoaction1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    //将Drive端的数据广播到Executor中
    //调用sc上的广播方法
    //广播变量的引用（还在Driver端）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    //创建RDD 读取访问日志
    val accessLines: RDD[String] = sc.textFile(args(1))

    //处理逻辑 在Executor中执行
    val func = (line: String) => {
      val fields: Array[String] = line.split("[|]")
      val ip: String = fields(1)
      //将ip转成十进制
      val ipNum: Long = MyUtils.ip2Long(ip)
      //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      val rulesExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //查找
      var province = "未知"
      val index: Int = MyUtils.binarySearch(rulesExecutor, ipNum)
      if (index != -1) {
        province = rulesExecutor(index)._3
      }
      (province, 1)
    }

    //整理数据
    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)

    //聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)

    //打印
    val r: Array[(String, Int)] = reduced.collect()
    println(r.toBuffer)

    sc.stop()
  }
}
