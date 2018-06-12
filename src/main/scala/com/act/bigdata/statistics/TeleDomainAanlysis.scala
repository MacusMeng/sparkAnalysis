package com.act.bigdata.statistics

import java.io.{BufferedReader, InputStreamReader}
import java.util.Date
import java.util.zip.ZipInputStream

import com.act.bigdata.util.{DateUtil, SparkUtil, StringUtil}
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Meng Ruo on 2018/5/17  11:58.
  * 黑域名日志中访问域名的人数排行（通过手机号来计算）
  **/
object TeleDomainAanlysis {
  val appStart = System.currentTimeMillis()
  val logger = LoggerFactory.getLogger(CatchSuspectAnalysis.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val sc = SparkUtil.sparkInit("telephoneDomain analysis")

  def main(args: Array[String]): Unit = {
    val now = new Date()
    val rdd = StringUtil.readFromZip(sc._1, "/tmp/data/userinfo/*.zip").filter(x => {
      var flag = false
      try {
        flag = x.split(",").length > 3 && !x.contains("Tel[(null)]")
      } catch {
        case ex: Throwable => println(ex)
      }
      flag
    })
    val infoRdd = rdd.map((x: String) => x.split(",")(0) + "," + x.split(",")(2) + "," + x.split(",")(3))
    infoRdd.coalesce(1).saveAsTextFile("/tmp/mengr/tele/teleInfo/" + DateUtil.dateHformat.format(now))
    infoRdd.map(x => {
      var domain = StringUtil.getDomain(StringUtil.extractMessage(x.split(",")(1)), 1)
      val tele = StringUtil.extractMessage(x.split(",")(2))
      if (domain == null) domain = "noDomain"
      (domain, tele)
    }).distinct.map(x => (x._1, 1)).reduceByKey(_ + _).sortBy(_._2, false)
      .map(x => x._1 + "," + x._2).coalesce(1).saveAsTextFile("/tmp/mengr/tele/domainRank/" + DateUtil.dateHformat.format(now))

    sc._1.stop()
    val appEnd = System.currentTimeMillis()
    logger.warn("运行时间:" + (appEnd - appStart) / 1000 + "s")
  }

}
