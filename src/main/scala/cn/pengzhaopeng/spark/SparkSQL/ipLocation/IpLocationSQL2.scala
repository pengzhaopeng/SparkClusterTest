package cn.pengzhaopeng.spark.SparkSQL.ipLocation

import cn.pengzhaopeng.spark.SparkRDD.ipLocation.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * jon的代价太昂贵，而且非常慢，解决思路是将表缓存起来（广播变量）
  */
object IpLocationSQL2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    //取到HDFS中的ip规则
    import spark.implicits._

    val rulesLines: Dataset[String] = spark.read.textFile(args(0))
    //整理ip规则数据()
    val rluesDataset = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //手机ip端规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = rluesDataset.collect()
    //广播(必须使用sparkcontext)
    //将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)

    //创建RDD，读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(args(1))

    //整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      //将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ipDataFrame.createTempView("v_log")

    //定义一个自定义函数（UDF），并注册
    //该函数的功能是（输入一个IP地址对应的十进制，返回一个省份名称）
    spark.udf.register("ip2Province", (ipNum: Long) => {
      //查找ip规则（事先已经广播了，已经在Executor中了）
      //函数的逻辑是在Executor中执行的，怎样获取ip规则的对应的数据呢？
      //使用广播变量的引用，就可以获得
      val ipRulesInExcutor: Array[(Long, Long, String)] = broadcastRef.value
      //根据IP地址对应的十进制查找省份名称
      val index: Int = MyUtils.binarySearch(ipRulesInExcutor, ipNum)
      var province = "未知"
      if (index != -1) {
        province = ipRulesInExcutor(index)._3
      }
      province
    })

    //执行SQL
    val sql = "select ip2Province(ip_num) province, count(*) counts from v_log group by province order by counts desc";
    val result: DataFrame = spark.sql(sql)
    result.show()

    spark.stop()
  }
}
