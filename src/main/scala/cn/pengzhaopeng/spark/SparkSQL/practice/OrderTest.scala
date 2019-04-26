package cn.pengzhaopeng.spark.SparkSQL.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 订单测试
  */
object OrderTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("OrderTest").master("local[*]").getOrCreate()

    import spark.implicits._


    //加载tbDate数据
    val tbDateRdd: RDD[String] = spark.sparkContext.textFile("data/tbDate.txt")
    val tbDateDS: Dataset[tbDate] = tbDateRdd
      .map(_.split(","))
      .map(attr =>
        tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt,
          attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt,
          attr(8).trim().toInt, attr(9).trim().toInt)
      ).toDS


    //加载tbStock数据
    val tbStockRdd = spark.sparkContext.textFile("data/tbStock.txt")
    val tbStockDS = tbStockRdd
      .map(_.split(","))
      .map(attr => tbStock(attr(0), attr(1), attr(2)))
      .toDS

    //加载tbStockDetail数据
    val tbStockDetailRdd = spark.sparkContext.textFile("data/tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRdd
      .map(_.split(","))
      .map(attr =>
        taStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble))
      .toDS

//    tbDateDS.show(10)
//    tbStockDS.show(10)
//    tbStockDetailDS.show(10)

    //注册表
    tbDateDS.createOrReplaceTempView("tbDate")
    tbStockDS.createOrReplaceTempView("tbStock")
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

    // 1.计算所有订单中每年的销售单数、销售总额
    val sql_1 = "SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a  JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear"

    //计算所有订单每年最大金额订单的销售额
    val sql_2 = "";

    val sqlDf: DataFrame = spark.sql(sql_1)
    sqlDf.show()




    spark.stop()
  }


  case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

  case class taStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double,
                           amount: Double) extends Serializable

  case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int,
                    weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

}
