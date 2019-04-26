package cn.pengzhaopeng.spark.SparkSQL.topTearcher

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLFavTeacher {
  def main(args: Array[String]): Unit = {

    val topN = 2

    val spark: SparkSession = SparkSession
    .builder()
    .appName("SQLFavTeacher")
    .master("local[4]")
    .getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.read.textFile(args(0))

    val df: DataFrame = lines.map(line => {
      val tIndex: Int = line.lastIndexOf("/") + 1
      val teacher: String = line.substring(tIndex)
      val host: String = new URL(line).getHost
      //学科的index
      val sIndex: Int = host.indexOf(".")
      val subject: String = host.substring(0, sIndex)
      (subject, teacher)
    }).toDF("subject", "teacher")

    df.createTempView("v_sub_teacher")

    //该学科下的老师的访问次数
//    val temp1: DataFrame = spark.sql("SELECT subject, teacher, count(*) counts FROM v_sub_teacher GROUP BY subject, teacher order by subject,counts desc")
    val temp1: DataFrame = spark.sql("SELECT subject, teacher, count(*) counts FROM v_sub_teacher GROUP BY subject, teacher")

    temp1.createTempView("v_temp_sub_teacher_counts")
//    val temp2 = spark.sql(s"SELECT * FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk, rank() over(order by counts desc) g_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")

    val temp2 = spark.sql(s"SELECT *, row_number() over(order by counts desc) g_rk FROM " +
      s"(SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 " +
      s"WHERE sub_rk <= $topN")


    temp2.show()

    spark.stop()

  }
}
