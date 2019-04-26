package cn.pengzhaopeng.spark.SparkSQL.ipLocation

import cn.pengzhaopeng.spark.SparkRDD.ipLocation.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IpLocationSQL1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IpLocationSQL1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //取HDFS 中ip规则
    val rulesLines: Dataset[String] = spark.read.textFile(args(0))
    //整理规则数据
    val ruleDataFrame: DataFrame = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")

    //读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(args(1))
    val ipDataFrame: DataFrame = accessLines.map(log => {
      val fields: Array[String] = log.split("[|]")
      val ip: String = fields(1)
      //将ip转成十进制
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    //注册视图
    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    //SQL
    val sql = "select province, count(*) counts from v_ips join v_rules on (ip_num >= snum and ip_num<= enum) group by province order by counts desc";
    val result: DataFrame = spark.sql(sql)
    result.show()

    spark.stop()
  }
}
