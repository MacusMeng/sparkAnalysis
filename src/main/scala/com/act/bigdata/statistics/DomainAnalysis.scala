package com.act.bigdata.statistics

import com.act.bigdata.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

/**
  * Created by Meng Ruo on 2018/5/21  20:01.
  **/
object DomainAnalysis {
  val appStart = System.currentTimeMillis()
  val logger = LoggerFactory.getLogger(DomainAnalysis.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val sc = SparkUtil.sparkInit("domain analysis")

  def main(args: Array[String]): Unit = {
    val data = sc._1.textFile("/tmp/mengr/blackDomain/*/*/*")
    data.map(x=>x.split(",")(0)).distinct().coalesce(1).saveAsTextFile("/tmp/data/domain")
    sc._1.stop()
    val appEnd = System.currentTimeMillis()
    logger.warn("运行时间:" + (appEnd - appStart) / 1000 + "s")
  }
}
