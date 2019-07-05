package cn.edu.nju.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * Created by thpffcj on 2019-07-02.
  */
object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

//    fromCollection(env)
//    textFile(env)
//    csvFile(env)
//    readRecursiveFiles(env)
    readCompressionFiles(env)
  }

  def readCompressionFiles(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/thpffcj/Public/data/compression"
    env.readTextFile(filePath).print()
  }

  def readRecursiveFiles(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/thpffcj/Public/data/nested"
    val parameters = new Configuration()
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filePath).withParameters(parameters).print()
  }

  case class MyCaseClass(name:String, age:Int)

  def csvFile(env: ExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._
    val filePath = "file:///Users/thpffcj/Public/data/people.csv"

    env.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()

    env.readCsvFile[(String, Int)](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()

    env.readCsvFile[MyCaseClass](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()

    env.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name", "age", "work")).print()
  }

  def textFile(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/thpffcj/Public/data/hello.txt"
    env.readTextFile(filePath).print()
  }

  def fromCollection(env: ExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }
}
