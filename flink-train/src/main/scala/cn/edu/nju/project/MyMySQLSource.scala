package cn.edu.nju.project

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
  * Created by thpffcj on 2019-07-11.
  */
class MyMySQLSource extends RichParallelSourceFunction[mutable.HashMap[String, String]] {

  var connection:Connection = null
  var ps:PreparedStatement = null

  // open：建立连接
  override def open(parameters: Configuration): Unit = {

    val url = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val password = "00000000"
    connection = DriverManager.getConnection(url, user, password)

    val sql = "select user_id, domain from user_domain_config"
    ps = connection.prepareStatement(sql)
  }

  // 释放资源
  override def close(): Unit = {
    if (ps != null) {
      ps.close()
    }

    if (connection != null) {
      connection.close()
    }
  }

  /**
    * 此处是代码的关键：要从MySQL表中把数据读取出来转成Map进行数据的封装
    * @param ctx
    */
  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {

    val resultMap = new mutable.HashMap[String, String]()

    val result = ps.executeQuery()
    while (result.next()) {
      val userId = result.getString(1)
      val domain = result.getString(2)
      resultMap.put(domain, userId)
    }
    ctx.collect(resultMap)
  }

  override def cancel(): Unit = {}
}
