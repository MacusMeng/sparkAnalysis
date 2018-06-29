package com.act.bigdata.dao

import com.act.bigdata.dbProperties.{DataAccess, DataUtils, DatabaseConnectionPool}
import com.act.bigdata.entity.Entity.WebClass

/**
  * Created by Meng Ruo on 2018/6/12  17:41.
  **/
class WebClassDao extends DataAccess(conn = DatabaseConnectionPool.getConnection) {
  def insert(list: Traversable[WebClass]): Unit = {
    val params = list.map(x => Array(x.url, x.category, x.title, x.keyword, x.content))
    this.execBatch("merge into domain_webclass_lib auth using (select ? domain  from dual) tmp on (auth" +
      ".domain=tmp.domain)" +
      " WHEN MATCHED THEN" +
      " UPDATE SET auth.webclass=?,auth.domain_content_title=?,auth.domain_content_keyword=?,auth" +
      ".domain_content=?" +
      " WHEN NOT MATCHED THEN" +
      " INSERT (domain,webclass, domain_content_title, domain_content_keyword, domain_content) VALUES " +
      "(?,?,?,?,?)", params)
  }
}

object WebClassDao extends DataUtils[WebClassDao] {
  override def getDao: WebClassDao = new WebClassDao
}
