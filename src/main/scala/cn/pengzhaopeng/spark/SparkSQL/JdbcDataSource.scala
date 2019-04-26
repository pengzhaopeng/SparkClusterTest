package cn.pengzhaopeng.spark.SparkSQL

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正mysql的数据吗？
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/taobao",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "orders",
        "user" -> "root",
        "password" -> "123456a")
    ).load()

    //    logs.printSchema()
    //    logs.show()

    //    val filtered: Dataset[Row] = logs.filter(r => {
    //      r.getAs[Int]("count") <= 3
    //    })
    //    filtered.show()

    //    val r: Dataset[Row] = logs.filter($"count" <= 3)
    //    val unit: Dataset[Row] = logs.where($"count" <= 3)
    //    r.show()

        val r: Dataset[Row] = logs.where($"count" <= 3)
        val result: DataFrame = r.select($"count" * 3 as "count1",$"status")
    //    result.show()

    val props = new Properties()
    props.put("user","root")
    props.put("password","123456a")

  }
}
