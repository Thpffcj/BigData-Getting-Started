package cn.edu.nju.course08

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.{SequenceFileWriter, StringWriter}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
  * Created by thpffcj on 2019-07-07.
  */
object FileSystemSinkApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost", 9999)

    val filePath = "file:///Users/thpffcj/Public/data"
    val sink = new BucketingSink[String](filePath)
    sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter[String]())
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

    data.addSink(sink)
    env.execute("FileSystemSinkApp")
  }
}
