package cn.edu.nju

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * 使用Scala开发Flink的批处理应用程序
  * Created by thpffcj on 2019-06-28.
  */
object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {

    val input = "file:///Users/thpffcj/Public/data/hello.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    // 引入隐式转换
    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1).print()
  }

  /**
    * hadoop	welcome
    * hadoop	hdfs	mapreduce
    * hadoop	hdfs
    *
    * hadoop
    * hdfs
    * hadoop
    * welcome
    * hadoop
    * hdfs
    * mapreduce
    *
    * hadoop
    * hdfs
    * hadoop
    * welcome
    * hadoop
    * hdfs
    * mapreduce
    *
    * (hadoop,1)
    * (hdfs,1)
    * (mapreduce,1)
    * (hadoop,1)
    * (welcome,1)
    * (hadoop,1)
    * (hdfs,1)
    *
    * (hdfs,2)
    * (hadoop,3)
    * (mapreduce,1)
    * (welcome,1)
    */
}
