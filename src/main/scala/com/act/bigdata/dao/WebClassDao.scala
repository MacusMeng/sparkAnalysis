package com.act.bigdata.dao

import com.act.bigdata.dbProperties.{DataAccess, DataUtils, DatabaseConnectionPool}
import com.act.bigdata.entity.Entity.WebClass

/**
  * Created by Meng Ruo on 2018/6/12  17:41.
  **/
class WebClassDao  extends DataAccess(conn = DatabaseConnectionPool.getConnection) {
  def insert(list:Array[WebClass]): Unit ={
    this.execBatch("insert into domain_webclass_lib (domain,webclass,domain_content_title,domain_content_keyword,domain_content) values(?,?,?,?,?)",Traversable(list))
  }
}
object WebClassDao extends DataUtils[WebClassDao] {
  override def getDao: WebClassDao = new WebClassDao
}
