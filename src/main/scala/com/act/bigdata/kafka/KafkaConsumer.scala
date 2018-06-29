package com.act.bigdata.kafka


import com.act.bigdata.dao.WebClassDao
import com.act.bigdata.entity.Entity.WebClass
import com.act.bigdata.util.SparkInitUtil
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

/**
  * Created by Meng Ruo on 2018/6/12  16:39.
  **/
object KafkaConsumer {
  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(SparkInitUtil.sparkInit("kafka Consumer"), Seconds(10))

    val brokers = "actcdh02:9092,actcdh03:9092,actcdh04:9092,actcdh05:9092,actcdh06:9092"
    val topics = "hqytest"
    val groupName = "saveO"
    val num_thread = 10
//    val kafkaParams = Map("metadata.broker.list" -> brokers, "group.id" -> groupName)
    val messages = KafkaUtils.createStream(ssc, brokers, groupName, Map(topics -> num_thread))

    val lines = messages.map(_._2).filter(x => x.contains("{") && x.contains("}"))
    val words = lines.flatMap(x => {
      val list = new ArrayBuffer[WebClass]
      val json = JSON.parseFull(x)
      json match {
        case Some(map: Map[String, String]) => {
          var key = ""
          if (map.contains("meta")) {
            key = map("meta")
          }
          list += WebClass(map("url"), map("category"), key, map("title"), map("content"),map("url"), map("category"), key, map("title"), map("content"))
        }
        case None => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }
      list
    })
    words.foreachRDD(x => {
      WebClassDao.exec(dao => dao.insert(x.collect()))
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
