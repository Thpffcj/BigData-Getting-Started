package cn.edu.nju

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * Created by thpffcj on 2019-07-04.
  */
object DataSetSinkApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = 1.to(10)
    val text = env.fromCollection(data)

    val filePath = "file:///Users/thpffcj/Public/data/sink-out"

    text.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(2)

    env.execute("DataSetSinkApp")
  }
}
