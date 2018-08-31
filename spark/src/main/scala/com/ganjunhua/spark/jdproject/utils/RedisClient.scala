package com.ganjunhua.spark.jdproject.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisClient {
  private var MAX_IDLE: Int = 200
  private var TIMEOUT: Int = 10000
  private var TEST_ON_BORROW: Boolean = true

  lazy val conf: JedisPoolConfig = {
    val conf = new JedisPoolConfig
    conf.setMaxIdle(MAX_IDLE)
    conf.setTestOnBorrow(TEST_ON_BORROW)
    conf
  }
  lazy val pool = new JedisPool(
    conf,
    KafkaProperties.redis_server,
    KafkaProperties.redis_port,
    TIMEOUT
  )
  lazy val hook = new Thread {
    override def run() = {
      println("Execute hook thread:" + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run())
}
