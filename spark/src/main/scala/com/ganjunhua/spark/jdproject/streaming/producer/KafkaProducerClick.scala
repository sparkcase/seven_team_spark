package com.ganjunhua.spark.jdproject.streaming.producer

import java.util.Properties

import com.ganjunhua.spark.jdproject.utils.KafkaProperties
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 将t_click数据依次写入kafka中的 t_click主题中，
  * 每条数据写入间隔为 10毫秒，其中uid为key，click_time+”,”+pid为value
  *
  */
object KafkaProducerClick {
  def main(args: Array[String]): Unit = {
    var masterType: String = null
    var dataPath: String = null
    var appName: String = null

    val defaultProertie = defaultProperties()
    val topic: String = defaultProertie._5
    if (args.length > 2) {
      masterType = args(0)
      appName = args(1)
      dataPath = args(2)
    } else {
      masterType = defaultProertie._2
      appName = defaultProertie._3
      dataPath = defaultProertie._4
    }
    println("masterType:=" + masterType)
    val prop = defaultProertie._1
    val conf = new SparkConf().setMaster(masterType).setAppName(appName)
    val sc = new SparkContext(conf)

    val clickRDD = sc.textFile(dataPath + "/t_click.csv")

    clickRDD.foreachPartition(partitionOFRecords => {
      // 创建 producer
      val kafkaConfig = new ProducerConfig(prop)
      val produce = new Producer[String, String](kafkaConfig)
      partitionOFRecords.foreach(record => {
        val records = record.split(",")
        val key = records(0)
        val values = records(1) + "," + records(2)
        val message = new KeyedMessage[String, String](topic, key, values)
        produce.send(message)
        Thread.sleep(10)
      })
    })
    sc.stop()
  }

  def defaultProperties() = {
    // 运行模式
    val masterType = "local[*]"
    //运行appName
    val appName = this.getClass.getSimpleName
    //数据路径
    val dataPath = "data/jd"
    //获取主题
    val topic = KafkaProperties.kafkaClickTopic
    //获取 地址
    val brokers = KafkaProperties.kafkaAddr
    //配置 变量
    val prop = new Properties()
    prop.put("metadata.broker.list", brokers)
    prop.put("serializer.class", "kafka.serializer.StringEncoder")
    prop.setProperty("key.serializer", classOf[StringSerializer].getName)
    prop.setProperty("value.serializer", classOf[StringSerializer].getName)
    // 创建 kafka配置
    val kafkaConfig = new ProducerConfig(prop)
    //返回值
    (prop, masterType, appName, dataPath, topic)
  }
}
