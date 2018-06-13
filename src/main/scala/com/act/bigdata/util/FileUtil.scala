package com.act.bigdata.util

import java.io._
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object FileUtil {

  def upload(localDir: String, cloudDir: String): Unit = {
    try {
      println("hadoop上传文件开始...")
      // 获取一个conf对象
      val conf = new Configuration()
      val fileDir = new File(localDir)
      val files = fileDir.listFiles()
      var in: InputStream = null
      var fs: FileSystem = null
      var out: OutputStream = null
      for (file <- files) {
        // 本地文件存取的位置
        val LOCAL_SRC = file.getAbsolutePath
        // 存放到云端HDFS的位置
        val CLOUD_DEST = cloudDir + file.getName
        in = new BufferedInputStream(new FileInputStream(LOCAL_SRC))
        // 文件系统
        fs = FileSystem.get(URI.create(CLOUD_DEST), conf)
        // 输出流
        out = fs.create(new Path(CLOUD_DEST))
        // 连接两个流，形成通道，使输入流向输出流传输数据
        IOUtils.copyBytes(in, out, 1024, true)
      }
      in.close
      fs.close
      out.close
      println("hadoop上传文件结束...")
    } catch {
      case e: FileNotFoundException =>
        e.printStackTrace()
      case e: IllegalArgumentException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace
    }
  }

}
