package cn.pengzhaopeng.spark.Mongodb

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkMongoClusterSQL {

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName("MongoSparkRDD")
      .config("spark.mongodb.input.uri", "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200/mobike.bikes?readPreference=secondaryPreferred")
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200/mobike.result")
      .getOrCreate()

    val df: DataFrame = MongoSpark.load(session)

    df.createTempView("v_bikes")

    val result: DataFrame = session.sql("select * from v_bikes")

    result.show()

//    MongoSpark.save(result)

    session.stop()
  }
}
