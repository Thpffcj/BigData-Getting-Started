package cn.edu.nju.log

import java.sql.DriverManager

import java.sql.{Connection, PreparedStatement, DriverManager}

/**
  * Created by Thpffcj on 2018/5/7.
  * MySQL操作工具类
  */
object MySQLUtils {

  /**
    * 获取数据库连接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sparksql?user=root&password=000000")
  }

  /**
    * 释放数据库连接等资源
    * @param connection
    * @param pstmt
    */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]) {
    println(getConnection())
  }
}
