package cn.edu.nju.course05

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * Created by thpffcj on 2019-07-05.
  */
class CustomParallelSourceFunction extends ParallelSourceFunction[Long] {

  var count = 1l
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
