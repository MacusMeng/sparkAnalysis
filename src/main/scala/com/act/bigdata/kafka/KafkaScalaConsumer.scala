package com.act.bigdata.kafka

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import com.act.bigdata.dao.WebClassDao
import com.act.bigdata.entity.Entity.WebClass
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.parsing.json.JSON

/**
  * Created by hadoop on 17-6-30.
  */
object KafkaScalaConsumer {

  def ZK_CONN     = "172.31.134.2:2181"
  def GROUP_ID    = "saveToOracle"
  def TOPIC       = "hqytest"


  def main(args: Array[String]): Unit = {
    //println(" 开始了 ")

    val connector = Consumer.create(createConfig())

    val topicCountMap = new HashMap[String, Int]()
    topicCountMap.put(TOPIC, 3) // TOPIC在创建时就指定了它有3个partition
    val msgStreams: Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = connector.createMessageStreams(topicCountMap)
    println("# of streams is " + msgStreams.get(TOPIC).get.size)
    val threadPool:ExecutorService=Executors.newFixedThreadPool(3)

    var index = 0;
    for (stream <- msgStreams.get(TOPIC).get) {
      threadPool.execute(new ThreadDemo("consumer_"+index,stream))
      index+=1;
    }
  }

  class ThreadDemo(threadName:String,stream:KafkaStream[Array[Byte], Array[Byte]]) extends Runnable{
    override def run(): Unit = {

      val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator();
      val list = new ArrayBuffer[WebClass]
      while(it.hasNext()){
        val data : MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
        print("消费者名称："+threadName+" ")
        println("key ->["+new String(data.key)+"], message->["+new String(data.message)+"], " +
          "partition->["+data.partition+"], offset->["+data.offset+"]")
        val json = JSON.parseFull(new String(data.message))
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
      WebClassDao.exec(dao=>dao.insert(list.toArray))
    }
  }

  def createConfig(): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", ZK_CONN)
    props.put("bootstrap.servers","172.31.134.2:9092")
    props.put("group.id", GROUP_ID)
    props.put("zookeeper.session.timeout.ms", "5000")
    props.put("zookeeper.connection.timeout.ms","10000")
    props.put("auto.offset.reset", "smallest")
    props.put("auto.commit.interval.ms", "300")
    props.put("rebalance.backoff.ms","2000")
    props.put("rebalance.max.retries","10")
    props.put("auto.offset.reset", "smallest")
    new ConsumerConfig(props)
  }
}
