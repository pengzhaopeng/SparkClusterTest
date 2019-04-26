package cn.pengzhaopeng.spark.SparkStreaming

import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wuyufei on 06/09/2017.
  */

//单例对象
object createKafkaProducerPool {

  //用于返回真正的对象池
  def apply(brokerList: String, topic: String): GenericObjectPool[KafkaProducerProxy] = {
    val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
    //制定了你的kafka对象池的大小
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    //直接返回一个对象池
    new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
  }
}

object KafkaStreaming {


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    //创建topic
    val brobrokers = "192.168.2.4:9092,192.168.2.5:9092,192.168.2.5"
    val sourcetopic = "source";
    val targettopic = "target";

    //创建消费者组
    var group = "con-consumer-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );



    //ssc.sparkContext.broadcast(pool)

    //创建DStream，返回接收到的输入数据
    //在Kafka中记录读取偏移量
    var stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策咯
      LocationStrategies.PreferConsistent,
      //订阅策咯
      ConsumerStrategies.Subscribe[String, String](Array(sourcetopic), kafkaParam))


    //每一个stream都是一个ConsumerRecord
    stream.map(s => ("id:" + s.key(), ">>>>:" + s.value()))
      //迭代DStream中的RDD，将每一个时间点对于的RDD拿出来
      .foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        // kafka连接池
        val pool = createKafkaProducerPool(brobrokers, targettopic)
        // 从连接池里面去除一个kafka的连接
        val p = pool.borrowObject()

        //发送当前分区里面的每一个数据
        partitionOfRecords.foreach { message => System.out.println(message._2); p.send(message._2, Option(targettopic)) }

        //使用完了需要将kafka还回去
        pool.returnObject(p)

      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
