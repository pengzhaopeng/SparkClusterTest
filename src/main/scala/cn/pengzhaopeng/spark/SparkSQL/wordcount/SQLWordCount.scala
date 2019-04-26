package cn.pengzhaopeng.spark.SparkSQL.wordcount

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * SQL 和 DataFrame 的方式
  */
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SQLWordCount")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("hdfs://hadoop02:9000/wordcount.txt")

    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //注册视图
    words.createTempView("v_wc")

    //执行SQL
    val sql = "SELECT value word,COUNT(*) counts from v_wc GROUP BY word ORDER BY counts DESC"
    val result: DataFrame = spark.sql(sql)

    //执行SQL
    result.show()

    spark.stop()
  }
}
