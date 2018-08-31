package com.ganjunhua.spark.jdproject.utils

object KafkaProperties {
  val redis_server: String = "holiday-1"
  val redis_port: Int = 6379

  val kafkaServer: String = "holiday-1"
  val kafkaAddr: String = kafkaServer + ":9092"
  val kafkaClickTopic: String = "t_click"
  val kakfaOrderTopic: String = "t_order"
  val zookeeperAddr: String = kafkaServer + ":2181"
  val zookeeperGroupIDOFClick: String = "t_click"
  val zookeeperGroupIDOFOrder: String = "t_order"
}
