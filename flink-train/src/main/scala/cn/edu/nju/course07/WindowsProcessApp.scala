package cn.edu.nju.course07

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by thpffcj on 2019-07-06.
  */
object WindowsProcessApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    text.flatMap(_.split(","))
      .map(x => (1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .process(new MyProcessWindowFunction())
      .print().setParallelism(1)

    env.execute("WindowsReduceApp")
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(Int, Int), String, Tuple, TimeWindow] {

    def process(key: Tuple, context: Context, input: Iterable[(Int, Int)], out: Collector[String]): Unit = {
      var count = 0L
      for (in <- input) {
        count = count + 1
      }
      out.collect(s"Window ${context.window} count: $count")
    }
  }
}
