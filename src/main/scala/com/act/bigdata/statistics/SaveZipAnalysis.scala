package com.act.bigdata.statistics

import java.io.File

import com.act.bigdata.util.{CarbonDataUtil, FileUtil, ShellUtil, StringUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonContext, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

/**
  * Created by Meng Ruo on 2018/6/12  10:33.
  **/
object SaveZipAnalysis {
  val appStart = System.currentTimeMillis()
  val logger = LoggerFactory.getLogger(SaveZipAnalysis.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val sc = CarbonDataUtil.sparkInit("zip analysis")

  def main(args: Array[String]): Unit = {
    val files = getFiles(new File("/u12/ftp_crawer/"))
    var dates = Set[String]()
    for (file <- files) {
      val fileName = file.getName
      if (fileName.startsWith("userinfo")){
        val date = fileName.split("_")(2).substring(0, 8)
        dates += date
      }
    }
    for (d <- dates) {
      val newFile = new File("/u11/userinfo/" + d)
      if (!newFile.exists()) {
        newFile.mkdir()
      }
      //移动文件到上传目录
      val f= new File("/u12/ftp_crawer/userinfo*" + d+"*.zip")
      f.renameTo(newFile)
      logger.warn("=====移动文件到上传目录=====")
      //上传文件到hdfs
      CarbonDataUtil.exsitFile(sc._1, "/tmp/data/userinfo/" + d)
      FileUtil.upload("/u11/userinfo/" + d,"/* /tmp/data/userinfo/" + d)
      logger.warn("=====上传文件到hdfs=====")
      //上传完毕后删除文件
      val f1 = new File("/u11/userinfo/"+d)
      f1.renameTo(new File("/u11/userinfo/tmp/"))
      logger.warn("=====上传完毕后删除文件=====")
      //存储到CarbonData
      saveToCarbon(sc._2, d, StringUtil.readFromZip(sc._1, "/tmp/data/userinfo/" + d + "/*.zip"))
      logger.warn("=====存储到CarbonData=====")
    }
  }

  def saveToCarbon(cbc: CarbonContext, date: String, stringRDD: RDD[String]): Unit = {
    cbc.sql("use anti_fraud")
    val createTable = "create table if not exists t_aj_userinfo_" + date +
      " (SIP string, DIP string,URL string,Tel string,ID string,Account string,account_prefix string,account_subfix string) STORED BY 'org.apache.carbondata.format'"
    cbc.sql(createTable)

    val schemaString = "SIP,DIP,URL,Tel,ID,Account,account_prefix,account_subfix"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = stringRDD.map(x => {
      val lines = x.split(",")
      var tel = lines(3)
      var id = lines(4)
      if ("Tel[(null)]".equals(tel)) {
        tel = ""
      }
      if ("ID[(null)]".equals(id)) {
        id = ""
      }
      Row(lines(0), lines(1), lines(2), tel, id, lines(5), lines(6), lines(7))
    })
    val dataFrame = cbc.createDataFrame(rowRDD, schema)
    dataFrame.registerTempTable("t_aj_userinfo_tmp_" + date)

    cbc.sql("insert into  t_aj_userinfo_" + date + "select * from t_aj_userinfo_tmp_" + date)

    sc._1.stop()
    val appEnd = System.currentTimeMillis()
    logger.warn("运行时间:" + (appEnd - appStart) / 1000 + "s")
  }

  def getFiles(dir: File): Array[File] = {
    dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles)
  }
}
