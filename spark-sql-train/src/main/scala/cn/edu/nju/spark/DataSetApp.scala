package cn.edu.nju.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Thpffcj on 2018/5/3.
  * DataSet操作
  */
object DataSetApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DatasetApp")
      .master("local[2]").getOrCreate()

    //注意：需要导入隐式转换
    import spark.implicits._

    val path = "/Users/thpffcj/Public/data/sales.csv"

    //spark如何解析csv文件？
    val df = spark.read.option("header","true").option("inferSchema","true").csv(path)
    df.show

    val ds = df.as[Sales]
    ds.map(line => line.itemId).show

    ds.map(line => line.itemId)

    spark.stop()
  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
}
