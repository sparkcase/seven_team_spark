package com.ganjunhua.spark.mysqlutils

import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

object DbUtils {
  // create properties
  private val mysqlProperties = new Properties()
  //  get db.properties
  val path: String = Thread.currentThread().getContextClassLoader
    .getResource(DbUtilsBean.resourceName).getPath
  //loan db.properties
  mysqlProperties.load(new FileInputStream(path))
  // get db values
  val mysqlDriverName: String = mysqlProperties.getProperty(DbUtilsBean.mysqlDriverName)
  val mysqlUserName: String = mysqlProperties.getProperty(DbUtilsBean.mysqlUserName)
  val mysqlPassword: String = mysqlProperties.getProperty(DbUtilsBean.mysqlPassword)
  val mysqlDBName: String = mysqlProperties.getProperty(DbUtilsBean.mysqlDBName)
  val mysqlPort: String = mysqlProperties.getProperty(DbUtilsBean.mysqlPort)
  val mysqlDBType: String = mysqlProperties.getProperty(DbUtilsBean.mysqlDBType)
  val mysqlHost: String = mysqlProperties.getProperty(DbUtilsBean.mysqlHost)
  //define mysqlUrl
  val url: String = "jdbc:" + mysqlDBType + "://" + mysqlHost + ":" + mysqlPort + "/" + mysqlDBName
  //reflex load class
  classOf[com.mysql.jdbc.Driver]

  // return mysql Connection
  def getConnection(): Connection = {
    DriverManager.getConnection(url, mysqlUserName, mysqlPassword)
  }

  def justPrint(): Unit = {
    println("...........")
  }

  def main(args: Array[String]): Unit = {
    justPrint
  }
}
