package com.act.bigdata.util

import java.util.ResourceBundle

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.CarbonContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Meng Ruo on 2016/12/1.
  * 初始化spark
  */
object SparkUtil {
  val carbonHdfsPath =  ResourceBundle.getBundle("hdfs").getString("hdfs.carbon")
  val hiveHdfsStorePath = ResourceBundle.getBundle("hdfs").getString("hdfs.hive")

  def sparkInit(appName:String): (SparkContext,CarbonContext) ={
    val conf = new SparkConf
    conf.setAppName(appName)
    val sc = new SparkContext(conf)
    val cbc = new CarbonContext(sc, carbonHdfsPath)
    cbc.setConf("hive.metastore.schema.verification", "false")
    cbc.setConf("hive.metastore.warehouse.dir", hiveHdfsStorePath)
    cbc.setConf(HiveConf.ConfVars.HIVECHECKFILEFORMAT.varname, "false")
    (sc,cbc)
  }

  def exsitFile(sc: SparkContext, filePath: String) = {
    val path = new Path(filePath)
    val configuration = sc.hadoopConfiguration
    try {
      val hdfs = FileSystem.get(configuration)
      if (!hdfs.exists(path)){
        hdfs.createNewFile(path)
      }
    } catch {
      case ex: Throwable => println(ex)
    }
    false
  }
}
