package com.act.bigdata.kafka

import java.util
import java.util.Properties

import com.act.bigdata.dao.WebClassDao
import com.act.bigdata.entity.Entity.WebClass
import org.apache.kafka.clients.consumer. KafkaConsumer
import scala.collection.JavaConversions._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

/**
  * Created by hadoop on 17-6-30.
  */
object KafkaScalaConsumer {

  def ZK_CONN = "172.31.134.2:2181"

  def GROUP_ID = "saveToOracle"

  def TOPIC = "hqytest"


  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "172.31.134.2:9092")
    props.put("group.id", GROUP_ID)
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("session.timeout.ms", "10000")
    props.put("heartbeat.interval.ms", "3000")
    props.put("offsets.commit.timeout.ms", "10000")
    props.put("request.timeout.ms", "70000")
    props.put("fetch.max.wait.ms", "10000")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(TOPIC))
    while (true) {
      System.out.println("开始消费topic:" + TOPIC)
      val records = consumer.poll(100)
      System.out.println("当前消费数据量:" + records.count)
      val list = new ArrayBuffer[WebClass]
      for (record <- records) {
        try
          Thread.sleep(1000)
        catch {
          case e: InterruptedException =>
            e.printStackTrace()
        }
        System.out.println("partition:" + record.partition + "offset:" + record.offset)
        val json = JSON.parseFull(new String(record.value()))
        json match {
          case Some(map: Map[String, String]) => {
            var key = ""
            if (map.contains("meta")) {
              key = map("meta")
            }
            list += WebClass(map("url"), map("category"), key, map("title"), map("content"))
          }
          case None => println("Parsing failed")
          case other => println("Unknown data structure: " + other)
        }
      }
      WebClassDao.exec(dao => dao.insert(list.toArray))
    }
  }
}
