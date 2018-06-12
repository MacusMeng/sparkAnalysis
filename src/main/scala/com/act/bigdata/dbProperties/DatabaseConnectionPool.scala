package com.act.bigdata.dbProperties

import java.util.ResourceBundle
import com.jolbox.bonecp.{BoneCPConfig, BoneCP}
import java.sql.Connection
import org.slf4j.LoggerFactory

/**
  * Created by Yangjun on 2017/2/24.
  *
  * 数据库连接池
  */
object DatabaseConnectionPool {

  private val log = LoggerFactory.getLogger(DatabaseConnectionPool.getClass)
  /**
    * 连接池，采用懒加载方式启动
    */
  private lazy val pool = {
    val reader = ResourceBundle.getBundle("sql")
    Class.forName(reader.getString("db.driver"))
    val config = new BoneCPConfig
    config.setJdbcUrl(reader.getString("db.url"))
    config.setUsername(reader.getString("db.username"))
    config.setPassword(reader.getString("db.password"))
    config.setMaxConnectionsPerPartition(reader.getString("db.max_connection").toInt)
    config.setMinConnectionsPerPartition(reader.getString("db.min_connection").toInt)
    config.setPartitionCount(reader.getString("db.partition_count").toInt)
    log.info("Startup datebase... ")
    new BoneCP(config)
  }

  /**
    * 关闭数据库
    *
    */
  def shutdown {
    pool.shutdown
    log.info("Shutdown datebase... ")
  }

  /**
    * 获取一个数据库的连接
    *
    * @return 数据库连接
    */
  def getConnection = pool.getConnection

  /**
    * 查询数据库
    *
    * @param query 查询逻辑
    * @return 数据库连接
    */
  def doQuery(query: Connection => Unit) {
    val conn = getConnection
    try {
      query(conn)
    }
    finally {
      conn.close
    }
  }

  /**
    * 操作数据库
    *
    * @param execute 数据库操作逻辑
    * @return 数据库连接
    */
  def doUpdate(execute: Connection => Unit) {
    val conn = getConnection
    try {
      conn setAutoCommit false
      execute(conn)
      conn.commit
    } catch {
      case t: Throwable => {
        conn.rollback
        throw t
      }
    } finally {
      conn.close
    }
  }

}