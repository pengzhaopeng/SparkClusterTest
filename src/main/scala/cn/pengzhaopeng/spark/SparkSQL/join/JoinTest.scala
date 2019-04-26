package cn.pengzhaopeng.spark.SparkSQL.join

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Spark SQL join操作
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhoa,china", "2,laoduan,usa", "3,laoyang,jp"))

    //整理数据
    val df1: DataFrame = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: String = fields(0)
      val name: String = fields(1)
      val nationCode: String = fields(2)
      (id, name, nationCode)
    }).toDF("id", "name", "nation")

    //整理规则
    val nations: Dataset[String] = spark.createDataset(List("china,中国", "usa,美国"))
    val df2: DataFrame = nations.map(nation => {
      val fields: Array[String] = nation.split(",")
      val ename: String = fields(0)
      val cname: String = fields(1)
      (ename, cname)
    }).toDF("ename","cname")

    //第一种  创建视图
//    df1.createTempView("v_users")
//    df2.createTempView("v_nations")
//    val sql = "SELECT name,cname from v_users JOIN v_nations on nation=ename"
//    val r: DataFrame = spark.sql(sql)

    //第二种DSL的风格
    val r: DataFrame = df1.join(df2,$"nation" === $"ename","left_outer")

    r.show()

    spark.stop()

  }
}
