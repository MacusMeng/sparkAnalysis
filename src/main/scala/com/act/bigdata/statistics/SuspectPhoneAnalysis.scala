package com.act.bigdata.statistics

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import com.act.bigdata.util.{DateUtil, CarbonDataUtil, StringUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonContext
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Meng Ruo on 2018/5/22  21:13.
  **/
object SuspectPhoneAnalysis {
  val appStart = System.currentTimeMillis()
  val logger = LoggerFactory.getLogger(BlackLogAnalysis.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val sc = CarbonDataUtil.sparkInit("suspectPhone analysis")

  def main(args: Array[String]): Unit = {
    val startTime = args(0)
    val endTime = args(1)

    val startDate = DateUtil.dateSFormat.parse(startTime).getTime
    val endDate = DateUtil.dateSFormat.parse(endTime).getTime
    for (i <- startDate to endDate by 3600000 * 24) {
      val date = DateUtil.dateSFormat.format(i)
      val data = getBlackLog(sc._2, date)
      userLog(date, data)
    }
    sc._1.stop()
    val appEnd = System.currentTimeMillis()
    logger.warn("运行时间:" + (appEnd - appStart) / 1000 + "s")
  }

  //黑域名日志信息
  def getBlackLog(cbc: CarbonContext, startTime: String): RDD[(String, String)] = {
    val tableName = "t_marker_black_full_data_" + startTime
    val sql = "select host,user_ipv4 from " + tableName
    cbc.sql("use anti_fraud")
    cbc.sql(sql).rdd.filter(x => x(0) != null).map(x => (x.getString(1), x.getString(0))).distinct()
  }

  //用户日志
  def userLog(startTime: String, domains: RDD[(String, String)]): Unit = {
    var rdd = sc._1.binaryFiles("/tmp/data/userinfo/" + startTime + "/*.zip").flatMap(x => {
      val stream = new ZipInputStream(x._2.open())
      val list = ArrayBuffer[String]()
      try {
        while (stream.getNextEntry != null) {
          val buf = new BufferedReader(new InputStreamReader(stream))
          while (buf.readLine() != null) {
            list += buf.readLine()
          }
        }
      } catch {
        case ex: Throwable => println(ex)
      }
      list
    })

    val userInfo = rdd.filter(x => {
      var flag = false
      try {
        val line = x.split(",")
        flag = line.length > 4 && !x.contains("Tel[(null)]") && (!line(3).startsWith("13800") || !line(3).startsWith("13010") || !line(3).endsWith("500"))
      } catch {
        case ex: Throwable => println(ex)
      }
      flag
    }).map(x=>(StringUtil.extractMessage(x.split(",")(0)),StringUtil.extractMessage(x.split(",")(3))))

      userInfo.leftOuterJoin(domains).mapValues(x=>(x._1,x._2.getOrElse("noHost"))).filter(x=> !"noHost".equals(x._2._2))
      .map(x =>x._2._2+","+x._1+","+x._2._1).distinct().coalesce(1).saveAsTextFile("/tmp/mengr/suspectPhone/" + startTime)
  }
}
