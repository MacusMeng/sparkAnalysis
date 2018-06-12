package com.act.bigdata.util

import java.io.{BufferedReader, InputStream, InputStreamReader}

object HbaseFileUtil {
  /**
    * 获取文件字符串
    */
  @throws[Exception]
  def readStringFromHBase(in: InputStream,_charset: String): String = {
    val buffer = new StringBuffer
    var content = ""
    if (in == null) return content
    var charset = _charset
    if (_charset == null) {
      charset = "utf-8"
    }
    var readCharset = charset
    if ("gbk" == charset) readCharset = "ISO-8859-1"
    val sr = new InputStreamReader(in, readCharset)
    val bufferedReader = new BufferedReader(sr)
    var line = ""
    while ({
      line = bufferedReader.readLine
      line != null
    }) buffer.append(line)
    // 字符编码转换
    content = new String(buffer.toString.getBytes(readCharset), charset)
    content
  }
}
