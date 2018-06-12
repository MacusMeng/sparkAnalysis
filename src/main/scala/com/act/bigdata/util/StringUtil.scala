package com.act.bigdata.util

import java.io.{BufferedReader, InputStreamReader}
import java.util
import java.util.zip.ZipInputStream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Meng Ruo on 2018/5/17  11:58.
  */
object StringUtil {
  def isIp(url: String): Boolean = {
    try {
      var url1 = url.replaceAll("https://", "").replaceAll("http://", "")
      var domains = url1.split("\\/")(0).split("\\:")(0).split("\\.");
      //IP
      if (domains.length == 4) {
        val finded = domains.filter(t => {
          try {
            t.toInt < 256
          } catch {
            case e: Exception => {
              false
            }
          }
        })
        if (finded.size == 4) {
          return true
        } else {
          return false
        }
      } else {
        return false
      }
    } catch {
      case e: Exception => {
        return false
      }
    }
  }

  def getRootDomain(url: String): String = {
    var ROOT_DOMAINS = Array(".com.cn", ".net.cn", ".org.cn", ".gov.cn", ".edu.cn");
    var url1 = url.replaceAll("https://", "").replaceAll("http://", "")
    val t = url1.split("\\/")(0).split("\\:")
    var domain = t(0)
    if (isIp(domain)) {
      return domain
    } else {
      var finded = ROOT_DOMAINS.filter(t => {
        domain.endsWith(t)
      })
      if (finded.size > 0) {
        finded.head
      } else {
        return "." + domain.split("\\.")(domain.split("\\.").length - 1)
      }
    }
  }

  def getDomain(url: String, level: Int = 10): String = {
    //    var ROOT_DOMAINS = Array(".com.cn",".com",".cn",".xin",".shop",".top",".club",".ltd",".wang",".xyz",".site",".vip",".net",".cc",".fun",".online",".biz",".red",".link",".mobi",".info",".org",".net.cn",".org.cn",".gov.cn",".name",".ink",".pro",".work",".tv",".kim",".group",".tech",".store",".ren",".在线",".中文网",".我爱你",".中国",".网址",".公司",".网络",".集团");
    try {
      if (url != null && url.length > 1) {
        var url1 = url.replaceAll("https://", "").replaceAll("http://", "")
        if (isIp(url1)) {
          url1
        }
        var t = url1.split("\\/")(0).split(":")
        var domain = t(0)
        var port = ""
        if (t.length == 2) {
          port = ":" + t(1)
        }
        var rootDomain = getRootDomain(url)
        domain = domain.substring(0, domain.lastIndexOf(rootDomain))
        var arr = domain.split("\\.").reverse
        var n = 0
        var topdomain: String = ""
        if (arr.length > 0) {
          while (n < arr.length && n < level) {
            topdomain = arr(n) + "." + topdomain
            n = n + 1
          }
          return topdomain.substring(0, topdomain.length - 1) + rootDomain + port;
        } else {
          return null
        }
      } else {
        return null
      }
    } catch {
      case e: Exception => {
        println("域名解析错误:" + url + ":" + e.getMessage)
        return null
      }
    }
  }

  //生成MD5
  def getMd5(s: String) = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }

  //截取中括号里面的内容
  def extractMessage(msg: String): String = {
    val list = new util.ArrayList[String]
    var start = 0
    var startFlag = 0
    var endFlag = 0
    var i = 0
    while ( {
      i < msg.length
    }) {
      if (msg.charAt(i) == '[') {
        startFlag += 1
        if (startFlag == endFlag + 1) start = i
      }
      else if (msg.charAt(i) == ']') {
        endFlag += 1
        if (endFlag == startFlag) list.add(msg.substring(start + 1, i))
      }

      {
        i += 1
        i - 1
      }
    }
    String.join(",", list)
  }

  //将zip文件读取为rdd
  def readFromZip(sc: SparkContext, path: String): RDD[String] = {
    sc.binaryFiles(path).flatMap(x => {
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
  }
}
