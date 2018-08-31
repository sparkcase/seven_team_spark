package com.ganjunhua.spark.jdproject.streaming.producer

import java.util.Properties

import com.ganjunhua.spark.jdproject.utils.KafkaProperties
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 将t_order数据依次写入kafka中的 t_order主题中，
  * 每条数据写入间隔为 10毫秒，
  * 其中uid为key，uid+”,”+price + “，” + discount为value
  */
object KafkaProducerOrder {

  def main(args: Array[String]): Unit = {
    var masterType: String = null
    var dataPath: String = null
    var appName: String = null
    val defaultPropertie = defaultProperties()
    val topic: String = defaultPropertie._5

    if (args.length > 2) {
      masterType = args(0)
      appName = args(1)
      dataPath = args(2)
    } else {
      masterType = defaultPropertie._2
      appName = defaultPropertie._3
      dataPath = defaultPropertie._4
    }

    val prop = defaultPropertie._1
    val conf = new SparkConf().setMaster(masterType)
      .setAppName(appName)
    val sc = new SparkContext(conf)

    val orderRDD = sc.textFile(dataPath + "/t_order.csv")

    orderRDD.foreachPartition(partitionOFRecords => {
      val kafkaConfig = new ProducerConfig(prop)
      val producer = new Producer[String, String](kafkaConfig)
      partitionOFRecords.foreach(record => {
        val records = record.split(",")
        val key = records(0)
        //uid+”,”+price + “，” + discount
        val values = records(0) + "," + records(2) + "," + records(5)
        val message = new KeyedMessage[String, String](topic, key, values)
        producer.send(message)
        println(message)
        Thread.sleep(10)
      })
    })

    sc.stop()

  }

  def defaultProperties() = {
    val masterType = "local[*]"
    val appName = this.getClass.getSimpleName
    val dataPath = "data/jd"
    val topic = KafkaProperties.kakfaOrderTopic
    val brokers = KafkaProperties.kafkaAddr
    val prop = new Properties()
    prop.put("metadata.broker.list", brokers)
    prop.put("serializer.class", "kafka.serializer.StringEncoder")
    prop.setProperty("key.serializer", classOf[StringSerializer].getName)
    prop.setProperty("value.serializer", classOf[StringSerializer].getName)
    val kafkaConfig = new ProducerConfig(prop)
    (prop, masterType, appName, dataPath, topic)
  }
}
