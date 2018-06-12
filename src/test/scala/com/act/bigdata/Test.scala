package com.act.bigdata

import java.io.{File, FileWriter, InputStreamReader, PrintWriter}
import java.text.SimpleDateFormat

import com.act.bigdata.util.ShellUtil
import org.apache.log4j.{Level, Logger}

import scala.io.Source

/**
  * Created by Administrator on 2017/2/17.
  */
object Test {
  val dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val redisFormat = new SimpleDateFormat("yyyyMMddHH")
  val partitionFormat = new SimpleDateFormat("yyyy-MM-dd")
  System.setProperty("hadoop.home.dir", "E:\\tools\\hadoop-2.6.5")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    //    val sc = new SparkContext(sparkConf)

    //    var str = new StringBuffer();
    //    val data = DomainDao.exec(dao=>dao.getDomainList(),false).get.foreach(x=>str.append(x+"\n"))
    //    val pw = new PrintWriter(new FileWriter("E:\\data\\export.txt"))
    //    pw.write(str.toString)
    //    pw.flush()
    //    pw.close()

    //    val list =new  ArrayList[UrlBean]()
    //    val data = getRealUrl("https://www.178wangdai.com/", "178网贷", "武汉一七八投资管理有限公司")
    //    list.add(data)
    //    val map = new HashMap[String,String]()
    //    map.put("domain","域名")
    //    map.put("company","公司名称")
    //    map.put("name","名称")
    //    ExcelUtil.ImportExcel(list,"E:\\data\\scala.xls",map,"网贷平台")

    //    val files = getFiles(new File("E:\\data\\test\\"))
    //    val data = new StringBuffer()
    //    for (file <- files) {
    //      val lines = Source.fromFile(file)
    //      for (line <- lines.getLines()) {
    //        data.append(line+"\n")
    //      }
    //      file.delete()
    //    }
    //
    //    val pw = new PrintWriter(new FileWriter("E:\\data\\test\\result.txt"))
    //    pw.write(data.toString)
    //    pw.flush()
    //    pw.close()
    val s = "userinfo_172.16.3.3_20180518235933"
    println(s.split("_")(2).substring(0, 8))
    var set = Set[Int]()
    set+=1
    set+=2
    set+=1
    println(set)

//    import java.io.BufferedReader
//    val runtime = Runtime.getRuntime
//    try {
//      val br = new BufferedReader(new InputStreamReader(runtime.exec("ipconfig").getInputStream,"utf-8"))
//      //StringBuffer b = new StringBuffer();
//      var line: String = null
//      val b = new StringBuffer()
//      while ((line = br.readLine) != null) {
//        b.append(line + "\n")
//      }
//     println(b.toString)
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
  }

  def getFiles(dir: File): Array[File] = {
    dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles)
  }

}
