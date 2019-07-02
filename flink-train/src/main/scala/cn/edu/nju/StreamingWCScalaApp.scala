package cn.edu.nju

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 使用Scala开发Flink的实时处理应用程序
  * Created by thpffcj on 2019-06-29.
  */
object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    import org.apache.flink.api.scala._

//    text.flatMap(_.split(","))
//      .filter(_.nonEmpty)
//      .map((_, 1))
//      .keyBy(0)
//      .timeWindow(Time.seconds(5))
//      .sum(1).print()

    text.flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map(x => WC(x, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count").print()

    env.execute("StreamingWCScalaApp")
  }

  case class WC(word: String, count:Int)
}
