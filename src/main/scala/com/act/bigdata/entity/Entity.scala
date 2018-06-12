package com.act.bigdata.entity

import java.sql.Date

/**
  * @author mengruo 2018/5/16 11:46
  **/
object Entity {

  case class BlackDomain(id: String, webType: String, typeLink: String, url: String, createTime: String)

  case class AppBean(createTime: Date, fileMD5: String, updateTime: Date, piratic: Int, certMd5: String, name: String, ipackage: String, feature: String, scanResult: Int, size: Int, version: String)

  case class UrlBean(name: String, company: String, domain: String)

  case class WebClass(url: String, category: String, keyword: String, title: String, content: String)

}
