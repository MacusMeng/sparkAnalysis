package com.act.bigdata.dao

import com.act.bigdata.dbProperties.{DataAccess, DataUtils, DatabaseConnectionPool}

/**
  * @author mengruo on 2018/5/16 11:53
  **/
class DomainDao extends DataAccess(conn = DatabaseConnectionPool.getConnection){
  def getDomainsLike(string: String): List[String] ={
    this.queryForList("select url from T_AJ_FRAUD_BANKPHISHING where PHISHING_TYPE not like '%"+string+"%'")(x=>x.getString(1))
  }
  def getDomainList(): List[String] ={
    this.queryForList("select URL,PHISHING_TYPE from T_AJ_FRAUD_BANKPHISHING")(x=>x.getString(1)+","+x.getString(2))
  }
}
object DomainDao extends DataUtils[DomainDao]{
  override def getDao: DomainDao = new DomainDao
}