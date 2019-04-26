package cn.pengzhaopeng.spark.SparkStreaming

import cn.pengzhaopeng.spark.utils.{JedisSentinePoolUtil, NumberFormatUtils}
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, JedisSentinelPool}

object CalculateUtil {

  /**
    * 计算成交的金额
    *
    * @param fields
    * @return
    */
  def calculateItem(fields: RDD[Array[String]]) = {

    val filterRDD: RDD[Array[String]] = fields.filter(x =>
      x.length > 4 && NumberFormatUtils.checkIsType(x(4), "double")
    )

    val itemAndPrice: RDD[(String, Double)] = filterRDD.map(arr => {

      //分类
      val item = arr(2)
      //金额
      val double: Double = arr(4).toDouble
      (item, double)
    })
    //按商品分类进行聚合
    val reduced: RDD[(String, Double)] = itemAndPrice.reduceByKey(_ + _)

    //将当前批次的数据累加到Redis中
    //foreachPartition是一个Action
    //现在这种方式，jeids的连接是在哪一端创建的（Driver）
    //在Driver端拿Jedis连接不好
    reduced.foreachPartition(part => {
      //获取一个Jedis连接
      //这个连接其实是在Executor中的获取的
      //JedisConnectionPool在一个Executor进程中有几个实例（单例）
      val conn: Jedis = JedisSentinePoolUtil.getInstance().getResource
      part.foreach(t => {
        //一个连接更新多条数据
        conn.incrByFloat(t._1, t._2)
      })
      //将当前分区中的数据跟新完在关闭连接
      JedisSentinePoolUtil.close(conn);
    })


  }

}
