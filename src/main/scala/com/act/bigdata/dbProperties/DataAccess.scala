package com.act.bigdata.dbProperties

import java.io.Closeable
import java.sql.{Connection, PreparedStatement, ResultSet, Statement, Timestamp}

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by Yangjun on 2017/2/24.
  * 数据存取
  */
abstract class DataAccess(protected val conn: Connection) extends Closeable {

    private val log = LoggerFactory.getLogger(classOf[DataAccess])

    /**
      * 插入数据
      *
      * @param sql     SQL语句
      * @param params  参数列表
      * @param convert 主键转换方法
      * @return 转换结果
      */
    protected def insert[T](sql: String, params: Array[_ <: Any])(implicit convert: ResultSet => T) = {
        log.debug("Execute SQL: " + sql)
        val pstmt = conn prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        try {
            setParameters(pstmt, params)
            pstmt.executeUpdate
            val rs = pstmt.getGeneratedKeys
            try {
                rs.next
                convert(rs)
            } finally rs.close
        } finally pstmt.close
    }

    /**
      * 批量插入或更新数据
      *
      * @param sql    SQL语句
      * @param params 参数列表
      * @return 转换结果
      */
    protected def execBatch[T](sql: String, params: Traversable[Array[_ <: Any]]) = {
        log.debug("Execute SQL: " + sql)
        val pstmt = conn prepareStatement (sql)
        try {
            for (p <- params) {
                setParameters(pstmt, p)
                pstmt.addBatch()
            }
            pstmt.executeBatch
        } finally pstmt.close
    }

    /**
      * 更新数据
      *
      * @param sql    SQL语句
      * @param params 参数列表
      * @return 影响行数
      */
    protected def update(sql: String, params: Array[_ <: Any]) = {
        log.debug("Execute SQL: " + sql)
        val pstmt = conn prepareStatement (sql)
        try {
            setParameters(pstmt, params)
            pstmt.executeUpdate
        } finally pstmt.close
    }

    /**
      * 查询对象
      *
      * @param sql     SQL语句
      * @param params  参数列表
      * @param convert 结果集转换方法
      * @return 泛型对象
      */
    protected def queryForObject[T](sql: String, params: Array[_ <: Any])(implicit convert: ResultSet => T): Option[T] = {
        log.debug("Execute SQL: " + sql)
        query(sql, params)(rs => {
            if (rs.next) {
                val result = convert(rs)
                if (rs.next) {
                    val ex = new ResultsTooManyException
                    log.error(ex.getMessage)
                    throw ex
                } else result
            } else rs
        })
    }

    /**
      * 查询对象列表
      *
      * @param sql     SQL语句
      * @param params  参数列表
      * @param convert 结果集转换方法
      * @return 泛型对象列表
      */
    protected def queryForList[T](sql: String, params: Array[_ <: Any]= Array.empty)(implicit convert: ResultSet => T): List[T] = {
        query(sql, params)(rs => {
            var results = List[T]()
            while (rs.next) {
                results = results :+ convert(rs)
            }
            results
        }) match {
            case Some(x) => x
            case None => List.empty
        }
    }

    /**
      * 查询对象映射
      *
      * @param sql     SQL语句
      * @param params  参数列表
      * @param convert 结果集转换方法
      * @return 泛型对象映射
      */
    protected def queryForMap[K, V](sql: String, params: Array[_ <: Any])(implicit convert: ResultSet => (K, V)): Map[K, V] = {
        query(sql, params)(rs => {
            var results = Map[K, V]()
            while (rs.next) {
                results += convert(rs)
            }
            results
        }) match {
            case Some(x) => x
            case None => Map.empty
        }
    }

    /**
      * 查询
      *
      * @param sql    SQL语句
      * @param params 参数列表
      * @return 查询结果
      */
    private def query[T](sql: String, params: Array[_ <: Any])(executor: ResultSet => T): Option[T] = {
        log.debug("Execute SQL: " + sql)
        val pstmt = conn prepareStatement (sql)
        try {
            setParameters(pstmt, params)
            val rs = pstmt.executeQuery
            val result = Try(executor(rs))
            rs.close
            result match {
                case Success(x) => Option(x)
                case Failure(e) =>
                    e.printStackTrace
                    None
            }
        } finally pstmt.close
    }

    /**
      * 插入参数
      *
      * @param pstmt  预编译声明
      * @param params 参数列表
      */
    private def setParameters(pstmt: PreparedStatement, params: Array[_ <: Any]) {
        for (i <- 1 to params.length) {
            pstmt setObject(i, params(i - 1))
        }
    }

    def close(): Unit = {
        conn.close
    }

    def setAutoCommit(autoCommit: Boolean) = conn.setAutoCommit(autoCommit)

    def rollback = conn.rollback

    def commit = conn.commit

    /**
      * 结果值读取器
      */
    implicit class ResultValueUtils(val rs: ResultSet) {
        def get[T >: Null](colName: String)(implicit t:Manifest[T]): T = {
            Try(ResultValueGetter.get[T](rs, colName)) match {
                case Success(x) => x
                case Failure(e) => 
                    e.printStackTrace
                    null
            }
        }
    }
}

object ResultValueGetter {
    def get[T >: Null](rs: ResultSet, colName: String)(implicit t:Manifest[T]): T = {
        if (t.runtimeClass == classOf[String]) {
            rs.getString(colName).asInstanceOf[T]
        } else if(t.runtimeClass == classOf[java.lang.Short] || t.runtimeClass == classOf[Short]) {
            rs.getShort(colName).asInstanceOf[T]
        } else if(t.runtimeClass == classOf[java.lang.Integer] || t.runtimeClass == classOf[Int]) {
            rs.getInt(colName).asInstanceOf[T]
        } else if(t.runtimeClass == classOf[java.lang.Long] || t.runtimeClass == classOf[Long]) {
            rs.getLong(colName).asInstanceOf[T]
        } else if(t.runtimeClass == classOf[java.lang.Double] || t.runtimeClass == classOf[Double]) {
            rs.getDouble(colName).asInstanceOf[T]
        } else if(t.runtimeClass == classOf[java.lang.Boolean] || t.runtimeClass == classOf[Boolean]) {
            rs.getBoolean(colName).asInstanceOf[T]
        } else if(t.runtimeClass == classOf[Timestamp]) {
            rs.getTimestamp(colName).asInstanceOf[T]
        } else {
            throw new Exception("类型" + t.runtimeClass + "不支持")
        }
    }
}


/**
  * 结果太多异常
  *
  */
class ResultsTooManyException extends Exception("Returned too many results.") {}

abstract class DataUtils[A <: DataAccess] {
    def exec[T](executor: A => T, isTry: Boolean = false, trans: Boolean = false): Option[T] = {
        val dao = getDao
        dao.setAutoCommit(!trans)
        val result = Try(executor(dao)) match {
            case Success(x) => {
                if (trans) dao.commit
                Option(x)
            }
            case Failure(e) => {
                if (trans) dao.rollback
                e.printStackTrace
                if (isTry) None else throw e
            }
        }
        dao.close
        result
    }

    def getDao: A
}