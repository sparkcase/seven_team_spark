package com.ganjunhua.spark.jdproject.streaming.consume

import com.ganjunhua.spark.jdproject.utils.{KafkaProperties, RedisClient}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}


object KafkaConsumeOrder {
  var masterType: String = "local[*]"
  var appName: String = this.getClass.getSimpleName
  var checkPointPath: String = "file:///D:/spark/order"
  val orderHashKey = "app::users::order"

  def main(args: Array[String]): Unit = {
    if (args.length > 2) {
      masterType = args(0)
      appName = args(1)
      checkPointPath = args(2)
    }
    val ssc = StreamingContext.getOrCreate(checkPointPath, functionCreateSSC)
    ssc.start()
    ssc.awaitTermination()
  }

  def functionCreateSSC(): StreamingContext = {
    val topic = Map(KafkaProperties.kakfaOrderTopic -> 1)
    val conf = new SparkConf().setAppName(appName).setMaster(masterType)
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkPointPath)
    val initData = ssc.sparkContext.parallelize(List[(String, Double)]())
    val kafkaStream = KafkaUtils.createStream(ssc,
      KafkaProperties.zookeeperAddr,
      KafkaProperties.zookeeperGroupIDOFOrder,
      topic)
    val orderData = kafkaStream.map(line => {
      val key = line._1
      val values = line._2.split(",")(2)
      (key, values.toDouble)
    })
    val orderSum = orderData.mapWithState(StateSpec.function(newMapWithState).initialState(initData))
    orderSum.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOFRecords => {
        val jedis = RedisClient.pool.getResource
        partitionOFRecords.foreach(records => {
          val uid = records._1
          val values = records._2
          jedis.hincrBy(orderHashKey, uid, values.toLong)
        })
      })
    })


    ssc
  }

  // 使用MapWithState 实现值的累加

  def newMapWithState = (word: String, one: Option[Double], state: State[Double]) => {
    val sum = one.getOrElse(0.0) + state.getOption().getOrElse(0.0)
    val outPut = (word, sum)
    state.update(sum)
    outPut
  }

}
