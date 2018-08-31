package com.ganjunhua.spark.jdproject.streaming.consume

import com.ganjunhua.spark.jdproject.utils.{KafkaProperties, RedisClient}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object KafkaConsumeClick {
  var masterType: String = "local[*]"
  var appName: String = this.getClass.getSimpleName
  var checkPointPath: String = "file:///D:/spark"
  val clickHashKey = "app::users::click"

  def main(args: Array[String]): Unit = {
    if (args.length > 3) {
      masterType = args(0)
      appName = args(1)
      checkPointPath = args(2)
    }
    val ssc = StreamingContext.getOrCreate(checkPointPath, functionCreateSSC)
    ssc.start()
    ssc.awaitTermination()
  }

  def functionCreateSSC(): StreamingContext = {
    val topic = Map(KafkaProperties.kafkaClickTopic -> 1)
    val conf = new SparkConf().setMaster(masterType).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkPointPath)
    val initData = ssc.sparkContext.parallelize(List[(String, Int)]())
    println(appName)
    val kafkaStream = KafkaUtils.createStream(ssc,
      KafkaProperties.zookeeperAddr,
      KafkaProperties.zookeeperGroupIDOFClick,
      topic)
    val clickData = kafkaStream.map(line => {
      // 获取kafkaStream中的第二列值，分割成数组，取第二位
      val keyPid = "click" + line._2.split(",")(1)
      (keyPid, 1)
    })
    val clickCount = clickData.mapWithState(StateSpec.function(newMapWithState).initialState(initData))
    clickCount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOFRecords => {
        val jedis = RedisClient.pool.getResource
        partitionOFRecords.foreach(record => {
          try {
            val uid = record._1
            val clickCnt = record._2
            jedis.hincrBy(clickHashKey, uid, clickCnt)
          } catch {
            case e: Exception => println("error:=" + e)
          }
        })
        jedis.close()
      })
    })
    clickCount.print()
    ssc
  }

  // 使用MapWithState 实现值的累加
  def newMapWithState = (word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
    val outPut = (word, sum)
    state.update(sum)
    outPut
  }
}
