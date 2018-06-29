package com.act.bigdata.util

import java.io.{BufferedReader, FileOutputStream, InputStreamReader}
import util.control.Breaks._

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.io.output.StringBuilderWriter
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

/**
  * Created by Meng Ruo on 2018/6/12  8:59.
  **/
object ShellUtil {
  val logger = LoggerFactory.getLogger(ShellUtil.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)
  private val DEFAULT_TIMEOUT = 5 * 3600 * 1000

  def command(cmd: Array[String]): Unit = {
    val startTime = System.currentTimeMillis
//    logger.info("------------------Spark定时任务开始------------------")
    logger.info("调用任务:" + cmd.mkString(","))
    var process: Process = null
    var fis: BufferedReader = null
    var eis: BufferedReader = null
    val sw: StringBuilderWriter = new StringBuilderWriter()
    try {
      process = Runtime.getRuntime.exec(cmd)
      fis = IOUtils.toBufferedReader(new InputStreamReader(process.getErrorStream))
      eis = IOUtils.toBufferedReader(new InputStreamReader(process.getErrorStream))
      // timeout control
      var isBegin = true
      var isFinished = false
      breakable(
        while (true) {
          if (isBegin) {
            sw.append(IOUtils.LINE_SEPARATOR)
          }
          if (eis.ready) IOUtils.copy(eis, sw)
          try {
            isBegin = false
            process.exitValue
            isFinished = true
          } catch {
            case e: IllegalThreadStateException =>
              // process hasn't finished yet
              isFinished = false
              Thread.sleep(1000)
          }
        }
      )
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      IOUtils.closeQuietly(eis)
      IOUtils.closeQuietly(fis)
      if (process != null) process.destroy()
    }
//    logger.info("------------------Spark定时任务结束({}ms)------------------", System.currentTimeMillis - startTime)
  }

  private def isOverTime(startTime: Long) = System.currentTimeMillis - startTime >= DEFAULT_TIMEOUT


  import java.io.BufferedReader
  import java.io.IOException
  import java.io.OutputStreamWriter
  import java.io.PrintWriter

  @throws[InterruptedException]
  def exec(command: String): String = {
    var returnString = ""
    var pro:Process = null
    val runTime = Runtime.getRuntime
    if (runTime == null) System.err.println("Create runtime false!")
    try {
      pro = runTime.exec(command)
      val input = new BufferedReader(new InputStreamReader(pro.getInputStream))
      val output = new PrintWriter(new OutputStreamWriter(pro.getOutputStream))
      var line:String = null
      while ( {
        (line = input.readLine) != null
      }) returnString = returnString + line + "\n"
      input.close()
      output.close()
      pro.destroy()
    } catch {
      case ex: IOException =>
        Logger.getLogger(classOf[Nothing].getName).log(Level.ERROR, null, ex)
    }
    returnString
  }
}
