package cn.pengzhaopeng.spark.Mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * mongodb 整合spark
  * https://docs.mongodb.com/spark-connector/v2.0/scala-api/
  */
object SparkMongoClusterRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("MongoSparkRDD")
      .setMaster("local[*]")
      .set("spark.mongodb.input.uri", "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200/mobike.bikes?readPreference=secondaryPreferred")
      .set("spark.mongodb.output.uri", "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200/mobike.result")

    //readPreference=secondaryPreferred" 表示可以从 从节点读取数据，跟在从中设置rs.slaveOk()一样
    val sc = new SparkContext(conf)


    val docsRDD: MongoRDD[Document] = MongoSpark.load(sc)

    //先从mongdb过滤再取数据
//    val filtered: RDD[Document] = docsRDD.filter(doc => {
//      val age = doc.get("age")
//      if (null == age) {
//        false
//      } else {
//        val ageDouble = age.asInstanceOf[Double]
//        ageDouble >= 31
//      }
//    })

    val r: Array[Document] = docsRDD.collect()

    println(r.toBuffer)

    sc.stop()
  }
}
