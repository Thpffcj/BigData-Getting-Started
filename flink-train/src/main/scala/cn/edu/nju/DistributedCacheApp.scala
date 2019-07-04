package cn.edu.nju

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * step1：注册一个本地/HDFS文件
  * step2：在open方法中获取到分布式缓存的内容即可
  * Created by thpffcj on 2019-07-04.
  */
object DistributedCacheApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "file:///Users/thpffcj/Public/data/hello.txt"

    // step1：注册一个本地/HDFS文件
    env.registerCachedFile(filePath, "scala-dc")

    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    data.map(new RichMapFunction[String, String] {

      // step2：在open方法中获取到分布式缓存的内容即可
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("scala-dc")
        val lines = FileUtils.readLines(dcFile)

        /**
          * 此时会出现一个异常，Java集合和Scala集合不兼容的问题
          */
        import scala.collection.JavaConverters._
        for (ele <- lines.asScala) {
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).print()
  }
}
