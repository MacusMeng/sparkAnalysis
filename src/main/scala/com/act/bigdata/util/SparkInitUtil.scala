package com.act.bigdata.util

import org.apache.spark.{SparkConf, SparkContext}

object SparkInitUtil {
  def sparkInit(appName:String): SparkContext ={
    val conf = new SparkConf
    conf.setAppName(appName)
    new SparkContext(conf)
  }
}
