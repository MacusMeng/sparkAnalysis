package com.act.bigdata.statistics

import com.act.bigdata.util.{DateUtil, CarbonDataUtil, StringUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.CarbonContext
import org.slf4j.LoggerFactory

/**
  * @author mengruo
  *         2018/5/16 13:56
  *         根据360判定的黑域名为基础，在黑域名日志和灰域名日志中查找包含判定的黑域名信息
  **/
object BlackLogAnalysis {
  val appStart = System.currentTimeMillis()
  val logger = LoggerFactory.getLogger(BlackLogAnalysis.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val sc = CarbonDataUtil.sparkInit("blackDomain analysis")

  def main(args: Array[String]): Unit = {
    val startTime = args(0)
    val endTime = args(1)

    val startDate = DateUtil.dateSFormat.parse(startTime).getTime
    val endDate = DateUtil.dateSFormat.parse(endTime).getTime
    for (i <- startDate to endDate by 3600000 * 24) {
      val date = DateUtil.dateSFormat.format(i)
      val domains = getBlackDomain(sc._1)
      //      getBlackLog(sc._2, date, domains)
      //      getGreyLog(sc._1, date, domains)
      getLiveDomain(sc._2, date, domains)
    }
    sc._1.stop()
    val appEnd = System.currentTimeMillis()
    logger.warn("运行时间:" + (appEnd - appStart) / 1000 + "s")
  }

  //360判定的黑域名
  def getBlackDomain(sc: SparkContext): Map[String, String] = {
    val result = Map[String, String]()
    sc.textFile("/tmp/mengr/data/*").map(x => (StringUtil.getDomain(x.split(",")(0), 1), x.split(",")(1)))
      .distinct().collect().foreach(x => result ++ Map(x._1 -> x._2))
    result
  }

  //获取黑域名日志域名
  def getLiveDomain(cbc: CarbonContext, startTime: String, domains: Map[String, String]): Unit = {
    val tableName = "t_marker_black_full_data_" + startTime
    val sql = "select host from " + tableName
    cbc.sql("use anti_fraud")
    val  data =cbc.sql(sql).rdd.filter(x => x(0) != null && domains.contains(StringUtil.getDomain(x.getString(0), 1)))
      .map(x => x.getString(0) + "," + domains(StringUtil.getDomain(x.getString(0), 1)))
      if(data.count()>0){
        data.coalesce(1).saveAsTextFile("/tmp/mengr/liveDomain/" + startTime)
      }
  }

  //黑域名日志信息中包含判定的黑域名信息
  def getBlackLog(cbc: CarbonContext, startTime: String, domains: Array[String]): Unit = {
    val tableName = "t_marker_black_full_data_" + startTime
    val sql = "select host,APP_SERVER_IP_IPV4,APP_SERVER_PORT from " + tableName
    cbc.sql("use anti_fraud")
    val data = cbc.sql(sql).rdd
    try {
      data.filter(x => x(0) != null && x(1) != null && x(2) != null && domains.indexOf(StringUtil.getDomain(x.getString(0), 1)) > 0)
        .map(x => x.getString(0) + "," + x.getString(1) + "," + x.getString(2))
        .coalesce(1).saveAsTextFile("/tmp/mengr/blackDomain/" + startTime + "/black")
    } catch {
      case ex: Exception => throw ex
    }
  }

  //灰域名日志信息中包含判定的黑域名信息
  def getGreyLog(sc: SparkContext, startTime: String, domains: Array[String]): Unit = {
    val path = "/data/anti_fraud/" + startTime.substring(0, 6) + "/" + startTime.substring(6, startTime.length) + "/grey_fulldata_topic/"
    if (CarbonDataUtil.exsitFile(sc, path)) {
      val data = sc.textFile(path + "*.ok.csv")
      data.filter(x => domains.indexOf(StringUtil.getDomain(x.split("\001")(0))) > 0)
        .map(x => {
          var result = ""
          try {
            result = x.split("\001")(0) + "," + x.split("\001")(16) + "," + x.split("\001")(18)
          } catch {
            case ex: Throwable => println(ex)
          }
          result
        }).filter(x => x.length > 0).coalesce(1).saveAsTextFile("/tmp/mengr/blackDomain/" + startTime + "/grey")
    }
  }
}
