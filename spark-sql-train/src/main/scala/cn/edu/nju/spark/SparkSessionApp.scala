package cn.edu.nju.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Thpffcj on 2018/5/2.
  * SparkSession的使用
  */
object SparkSessionApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()

    val people = spark.read.json("D:/people.json")
    people.show()

    spark.stop()
  }
}
