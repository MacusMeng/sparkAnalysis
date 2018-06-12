package com.act.bigdata.dao

import com.act.bigdata.dbProperties.{DataAccess, DataUtils, DatabaseConnectionPool}
import com.act.bigdata.entity.Entity.AppBean

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Meng Ruo on 2018/5/18  18:08.
  **/
class AppDao extends DataAccess(conn = DatabaseConnectionPool.getConnection) {
  def insert(datas: Array[AppBean]): Unit = {
    this.execBatch("insert into mobile_app_info (fileMD5,certMd5,name,piratic,package,feature,scanResult,appsize,version,createTime,updateTime) values(?,?,?,?,?,?,?,?,?,?,?)", Traversable(datas))
  }
}

object AppDao extends DataUtils[AppDao] {
  override def getDao: AppDao = new AppDao
}
