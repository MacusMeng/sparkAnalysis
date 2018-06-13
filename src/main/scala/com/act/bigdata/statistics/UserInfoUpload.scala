package com.act.bigdata.statistics


import java.io.File

import com.act.bigdata.util.{ShellUtil, CarbonDataUtil, StringUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonContext, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

/**
  * Created by Meng Ruo on 2018/6/12  10:33.
  **/
object UserInfoUpload {
  val appStart = System.currentTimeMillis()
  val logger = LoggerFactory.getLogger(UserInfoUpload.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val sc = CarbonDataUtil.sparkInit("userInfo upload")

  def main(args: Array[String]): Unit = {
    //TODO,上传文件到hdfs,scala调用shell命令
    val files = getFiles(new File("/u12/ftp_crawer/userinfo*"))
    var dates = Set[String]()
    for (file <- files) {
      val fileName = file.getName
      val date = fileName.split("_")(2).substring(0, 8)
      dates += date
    }
    for (d <- dates) {
      val newFile = new File("/u11/userinfo/" + d)
      if (!newFile.exists()) {
        newFile.createNewFile()
      }
      //移动文件到上传目录
      val mvCom = Array("mv", "/u12/ftp_crawer/userinfo*" + d + "*.zip", "/u11/userinfo/" + d)
      ShellUtil.command(mvCom)
      logger.warn("=====移动文件到上传目录=====")
      //上传文件到hdfs
      CarbonDataUtil.exsitFile(sc._1, "/tmp/data/userinfo/" + d)
      val upCom = Array("hadoop", "fs", "-put", "/u11/userinfo/" + d + "/*", "/tmp/data/userinfo/" + d)
      ShellUtil.command(upCom)
      logger.warn("=====上传文件到hdfs=====")
      //上传完毕后删除文件
      val deleCom = Array("mv", "/u11/userinfo/" + d ,"/u11/userinfo/tmp/")
      ShellUtil.command(deleCom)
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
    dataFrame.registerTempTable("t_aj_userinfo_" + date)

    cbc.sql("insert into  t_aj_userinfo_" + date + "select * from t_aj_userinfo_" + date)
  }

  def getFiles(dir: File): Array[File] = {
    dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles)
  }
}
