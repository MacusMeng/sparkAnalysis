package com.act.bigdata.util

import java.net.URI
import java.util.ResourceBundle

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

import scala.util.{Failure, Success, Try}

/**
  * Created by Yangjun on 2017/2/23.
  */
object JedisClient {
    private lazy val jedisPool = {
        val reader = ResourceBundle.getBundle("hdfs")
        val config = new GenericObjectPoolConfig
        config.setMaxIdle(20)
        config.setMaxTotal(100)
        config.setMinIdle(0)
        config.setMaxWaitMillis(-1)
        new JedisPool(config, URI.create(reader.getString("hdfs.redis")), 10000)
    }

    def getJedis: Jedis = {
        jedisPool.getResource
    }

    def exec[T](work : Jedis=>T): T ={
        val redis = getJedis
        Try(work(redis)) match {
            case Success(x) => {
                redis.close
                x
            }
            case Failure(e) => {
                redis.close
                throw e
            }
        }
    }
}
