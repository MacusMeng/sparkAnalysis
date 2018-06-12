package com.act.bigdata.util

import java.text.SimpleDateFormat

import java.util.{Calendar, Date}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by Administrator on 2017/4/13.
  */
object DateUtil {
  val dayFormatS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val dayFormats = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
  val dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val dateWebFormat = new SimpleDateFormat("yyyy/MM/dd")
  val dateSFormat = new SimpleDateFormat("yyyyMMdd")
  val dateHformat = new SimpleDateFormat("yyyyMMddHHmmss")
  val redisFormat = new SimpleDateFormat("yyyyMMddHH")
  val partitionFormat = new SimpleDateFormat("yyyy-M-d")

  case class DateObjectField(startTime: Date, endTime: Date)


  //时间区间
  def timePeriod(startPartition: Long, endPartition: Long): ArrayBuffer[String] = {
    var timeArray = new ArrayBuffer[String]()
    for (i <- startPartition to endPartition by 3600000 * 24) {
      timeArray += DateUtil.dateFormat.format(new Date(i))
    }
    timeArray
  }

  //获取明天的时间
  def getTomorrow(date: Date): Date = {
    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, 1)
    calendar.getTime
  }

  //获取所给时间的所在小时
  def formatTime(timeDate: String): String = {
    val date = dayFormatS.parse(timeDate)
    date.setMinutes(0)
    date.setSeconds(0)
    dayFormat.format(date)
  }

  //获取时间的前一个小时时间
  def getTimeOneHourAgo(date: Date): Date = {
    date.setHours(date.getHours - 1)
    date.setMinutes(0)
    date.setSeconds(0)
    date
  }


  //格式化含有微秒和毫秒的数据
  def formatMicrosecond(time: String): Long = {
    val i = time.indexOf('.')
    var format = 0l
    if (i != -1) {
      val mm = time.substring(i + 1)
      val ms = mm.toInt / 1000
      val times = time.substring(0, time.length - mm.length) + ms
      val dayFormatS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      format = dayFormatS.parse(times).getTime
    }
    else {
      val dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format = dayFormat.parse(time).getTime
    }
    format
  }

  //时间区间合并策略
  def mergeList(list1: ArrayBuffer[(Long, Long)], list2: ArrayBuffer[(Long, Long)]): ArrayBuffer[(Long, Long)] = {
    var result = list1
    for (i <- list2) {
      result = mergeItem(result, i)
    }
    result
  }

  def mergeItem(list1: ArrayBuffer[(Long, Long)], item: (Long, Long)): ArrayBuffer[(Long, Long)] = {
    val result = new ArrayBuffer[(Long, Long)]
    var temp = item
    for (i <- list1) {
      if (temp._2 < i._1) {
        result.append(temp)
        temp = i
      } else if (temp._1 > i._2) {
        result.append(i)
      } else {
        temp = (Math.min(i._1, temp._1), Math.max(i._2, temp._2))
      }
    }
    result += temp
  }

  /** 格式化时间 */
  def dataFormatTime(number: Number): String = {
    val result = new StringBuilder()
    if (number.isInstanceOf[Long] || number.isInstanceOf[Double] || number.isInstanceOf[Float]) {
      var d = 0l
      if (number.isInstanceOf[Long]) d = number.asInstanceOf[Long].longValue()
      else if (number.isInstanceOf[Double]) d = number.asInstanceOf[Double].longValue
      else d = number.asInstanceOf[Float].longValue
      // 处理小时
      var hour = 0l
      if (d >= 3600) {
        hour = d / 3600
        d = d - hour * 3600
        result.append(hour).append("小时")
      }
      // 处理分钟
      if (d >= 60) {
        val min = d / 60
        d = d - min * 60
        result.append(min).append("分钟")
      }
      else if (hour > 0) result.append(0).append("分钟")
      // 处理秒
      result.append(d).append("秒")
    }
    else if (number.isInstanceOf[Integer] || number.isInstanceOf[Byte]) {
      var d = 0
      if (number.isInstanceOf[Integer]) d = number.asInstanceOf[Integer]
      else d = number.asInstanceOf[Byte].intValue
      var hour = 0
      if (d >= 3600) {
        hour = d / 3600
        d = d - hour * 3600
        result.append(hour).append("小时")
      }
      if (d >= 60) {
        val min = d / 60
        d = d - min * 60
        result.append(min).append("分钟")
      }
      else if (hour > 0) result.append(0).append("分钟")
      result.append(d).append("秒")
    }
    else result.append(number)
    result.toString
  }
}
