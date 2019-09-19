package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 14:13
  * @description:
  * @since 1.0
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()
  config.setMaxTotal(20)
  config.setMaxIdle(10)

  private val pool = new JedisPool(config, "hadoop102",6379,1000,"000000")

  def getConnection():Jedis={
    pool.getResource
  }

}
