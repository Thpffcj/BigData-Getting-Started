package cn.edu.nju.project

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * Created by thpffcj on 2019-07-11.
  */
object MyMySQLSourceTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.addSource(new MyMySQLSource)
    data.print()

    env.execute("MyMySQLSourceTest")
  }
}
