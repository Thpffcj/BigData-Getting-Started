package cn.edu.nju

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer
// 隐式转换
import org.apache.flink.api.scala._

/**
  * Created by thpffcj on 2019-07-02.
  */
object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    mapFunction(env)
//    filterFunction(env)
//    mapPartitionFunction(env)
//    firstFunction(env)
//    flatMapFunction(env)
    distinctFunction(env)
  }

  def distinctFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data = env.fromCollection(info)

    data.flatMap(_.split(",")).distinct().print()
  }

  def flatMapFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data = env.fromCollection(info)
    data.map(_.split(",")).print()
    data.flatMap(_.split(",")).print()
    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
  }

  def firstFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((2, "Spring Boot"))
    info.append((3, "Linux"))
    info.append((4, "Vue"))

    val data = env.fromCollection(info)
    data.first(3).print()
    data.groupBy(0).first(2).print()
    data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
  }

  // DataSource 100个元素，把结果存储到数据库中
  def mapPartitionFunction(env: ExecutionEnvironment): Unit = {
    val students = new ListBuffer[String]
    for (i <- 1 to 100) {
      students.append("student: " + i)
    }

    val data = env.fromCollection(students).setParallelism(5)

    data.mapPartition(x => {

      val connection = DBUtils.getConnection()
      println(connection + "...")

      // TODO... 保存数据到DB

      DBUtils.returnConnection(connection)
      x
    }).print()
  }

  def filterFunction(env: ExecutionEnvironment): Unit = {
    env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .map(_ + 1)
      .filter(_ > 5).print()
  }

  def mapFunction(env: ExecutionEnvironment): Unit = {

    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // 对data中的每个元素做一个+1操作
    data.map((x:Int) => x + 1)
    data.map((x) => x + 1)
    data.map(x => x + 1)
    data.map(_ + 1).print()
  }
}
