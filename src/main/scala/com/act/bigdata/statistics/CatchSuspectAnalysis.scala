package com.act.bigdata.statistics

import com.act.bigdata.util.{DateUtil, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonContext
import org.slf4j.LoggerFactory

/**
  * @author mengruo
  *         特定端口（2828，3838）找出网站，并通过找到的网站中的用户ip再次查找用户访问的所有网站，
  *         根据共同访问最多的网站确定需要寻找的对象
  **/
object CatchSuspectAnalysis {
  val appStart = System.currentTimeMillis()
  val logger = LoggerFactory.getLogger(CatchSuspectAnalysis.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val sc = SparkUtil.sparkInit("suspect analysis")

  def main(args: Array[String]): Unit = {
    val startTime = args(0)
    val endTime = args(1)

    val startDate = DateUtil.dateSFormat.parse(startTime).getTime
    val endDate = DateUtil.dateSFormat.parse(endTime).getTime
    for (i <- startDate to endDate by 3600000 * 24) {
      val date = DateUtil.dateSFormat.format(i)
      blackSuspectData(sc, date)
    }
    sc._1.stop()
    val appEnd = System.currentTimeMillis()
    logger.warn("运行时间:" + (appEnd - appStart) / 1000 + "s")
  }

  def blackSuspectData(sc: (SparkContext, CarbonContext), startTime: String): Unit = {
    val tableName = "t_marker_black_full_data_" + startTime
    val sql = "select host,user_ipv4 from " + tableName
    sc._2.sql("use anti_fraud")
    val data = sc._2.sql(sql).rdd
    val allData = data.filter(x => x(0) != null && (x.getString(0).endsWith(":2828") || x.getString(0).endsWith(":3838")))
    if (allData.count() > 0) {
      try {
        val fData = allData.map(x => (x(1).toString, x(0).toString))
        val ips = fData.map(x => x._1).collect()
        if (ips.length > 0) {
          val temp = data.filter(x => x(0) != null && x(1) != null && ips.indexOf(x(1).toString) > 0)
          val sData = temp.map(x => (x(1).toString, x(0).toString))
          saveToHdfs(sData, fData, "/tmp/mengr/suspect/" + startTime + "/black")

          val path = "/data/anti_fraud/" + startTime.substring(0, 6) + "/" + startTime.substring(6, startTime.length) + "/grey_fulldata_topic/*.ok.csv"
          val gdata = sc._1.textFile(path)
          val sgData = gdata.filter(x => x.split("\001").length > 2 && ips.indexOf(x.split("\001")(1)) > 0)
            .map(x => (x.split("\001")(1), x.split("\001")(0)))
          saveToHdfs(sgData, fData, "/tmp/mengr/suspect/" + startTime + "/grey")
        }
      } catch {
        case ex: Throwable => println( ex)
      }
    }
  }

  def saveToHdfs(sData: RDD[(String, String)], fData: RDD[(String, String)], path: String): Unit = {
    sData.leftOuterJoin(fData).mapValues(x => (x._1, x._2.getOrElse("noDOmain")))
      .filter(x => !x._2._2.contains("noDomain") && !x._2._2.equals(x._2._1))
      .map(x => (x._2._1, (Array(x._2._2), 1)))
      .reduceByKey((x, y) => (x._1 ++ y._1, x._2 + y._2)).map(x => (x._1, x._2._1.distinct, x._2._2)).sortBy(_._2.length, false)
      .map(x => x._1 + "," + x._2.length + ",(" + x._2.mkString(",") + ")," + x._3).coalesce(1)
      .saveAsTextFile(path)
  }
}
