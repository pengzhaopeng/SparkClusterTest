package cn.pengzhaopeng.spark.SparkStreaming

import cn.pengzhaopeng.spark.Constant
import cn.pengzhaopeng.spark.utils.{JedisSentinePoolUtil, MyUtils, NumberFormatUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, JedisSentinelPool}

object CalculateUtil {


  /**
    * 计算成交总金额
    *
    * @param fields
    * @return
    */
  def calculateIncome(fields: RDD[Array[String]]) = {
    val priceRDD: RDD[Double] = fields.map(arr => {
      val price: Double = arr(4).toDouble
      price
    })
    //reduce是一个Action，会把结果返回到Driver端
    //将当前批次的总金额返回了
    val sum: Double = priceRDD.reduce(_ + _)
    //获取一个jedis连接
    val conn: Jedis = JedisSentinePoolUtil.getInstance().getResource
    //将历史值和当前的值进行累加
    conn.incrByFloat(Constant.TOTAL_INCOME, sum)
    //释放连接
    JedisSentinePoolUtil.close(conn)
  }


  /**
    * 计算成交的金额
    *
    * @param fields
    * @return
    */
  def calculateItem(fields: RDD[Array[String]]) = {

    //    val filterRDD: RDD[Array[String]] = fields.filter(x =>
    //      x.length > 4 && NumberFormatUtils.checkIsType(x(4), "double")
    //    )

    val itemAndPrice: RDD[(String, Double)] = fields.map(arr => {

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
      JedisSentinePoolUtil.close(conn)
    })


  }

  /**
    * 根据Ip地址计算归属地 然后计算区域成交金额
    *
    * @param filterRDD
    * @param broadcastRef
    */
  def calculateZone(fields: RDD[Array[String]], broadcastRef: Broadcast[Array[(Long, Long, String)]]) = {
    val provinceAandPrice: RDD[(String, Double)] = fields.map(arr => {
      val ip = arr(1)
      val price: Double = arr(4).toDouble
      val ipNum: Long = MyUtils.ip2Long(ip)
      //在Executor中获取到广播的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value
      //二分法查找
      val index: Int = MyUtils.binarySearch(allRules, ipNum)
      var province = "未知"
      if (index != -1) {
        province = allRules(index)._3
      }
      //省份 金额
      (province, price)
    })

    //按省份进行聚合
    val reduced: RDD[(String, Double)] = provinceAandPrice.reduceByKey(_ + _)
    //将数据更新到Redis
    reduced.foreachPartition(part => {
      val conn: Jedis = JedisSentinePoolUtil.getInstance().getResource
      part.foreach(t => {
        conn.incrByFloat(t._1, t._2)
      })
      JedisSentinePoolUtil.close(conn)
    })
  }

}
