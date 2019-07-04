package cn.edu.nju

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * 基于Flink编程的计数器开发三部曲
  * step1：定义计数器
  * step2：注册计数器
  * step3：获取计数器
  * Created by thpffcj on 2019-07-04.
  */
object CounterApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

//    data.map(new RichMapFunction[String, Long] {
//      var counter = 0l
//      override def map(value: String): Long = {
//        counter = counter + 1
//        println("counter : " + counter)
//        counter
//      }
//    }).setParallelism(5).print()

    val info = data.map(new RichMapFunction[String, String] {

      // step1：定义计数器
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // step2：注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    }).setParallelism(5)

    val filePath = "file:///Users/thpffcj/Public/data/sink-scala-count-out"
    info.writeAsText(filePath, WriteMode.OVERWRITE)
    val jobResult = env.execute("CounterApp")
    // step3：获取计数器
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")

    println("num: " + num)
  }
}
