package cn.pengzhaopeng.spark.SparkStreaming

import java.lang

import cn.pengzhaopeng.spark.utils.{IPUtils, NumberFormatUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * spark-kafka直连方式
  */
object DirectStream {

  def main(args: Array[String]): Unit = {

    val group = "first_group_peng"
    val topic = "my_orders"
    val conf: SparkConf = new SparkConf().setAppName("DirectStream").setMaster("local[2]")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    //广播参数 ip地址规则
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = IPUtils.broadcastIPRules(streamingContext,"F:\\IDEAProject\\ScalaDemo\\SparkClusterTest\\data\\ip.txt")

    //配置kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "acks" -> "all",
      "auto.offset.reset" -> "earliest", // lastest
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    //不止一个topic
    val topics = Array(topic)
    //用直连方式读取kafka中的数据，在Kafka中记录读取偏移量
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      //位置策略（如果kafka和spark程序部署在一起，会有最优位置）
      PreferConsistent,
      //订阅的策略（可以指定用正则的方式读取topic，比如my-ordsers-.*）
      Subscribe[String, String](topics, kafkaParams)
    )

    //迭代DStream中的RDD，将每一个时间点对于的RDD拿出来
    kafkaStream.foreachRDD { kafkaRDD =>
      if (!kafkaRDD.isEmpty()) {
        //获取该RDD对于的偏移量 只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
        val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //整理数据  //业务代码 数据参照order.log
        val lines: RDD[String] = kafkaRDD.map(_.value())
        val fields: RDD[Array[String]] = lines.map(_.split(" "))

        //过滤垃圾数据
        val filterRDD: RDD[Array[String]] = fields.filter(x =>
          x.length > 4 && NumberFormatUtils.checkIsType(x(4), "double")
        )

        //计算成交总金额
        CalculateUtil.calculateItem(filterRDD)

        //计算商品分类金额
        CalculateUtil.calculateIncome(filterRDD)

        //计算区域成交金额
        CalculateUtil.calculateZone(filterRDD,broadcastRef)

        //更新偏移量
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
