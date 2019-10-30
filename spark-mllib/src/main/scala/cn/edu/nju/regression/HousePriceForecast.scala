package cn.edu.nju.regression

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * Created by thpffcj on 2019/10/29.
 */
object HousePriceForecast {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("HousePriceForecast")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 加载文件
    val file = spark.read.format("csv")
      .option("sep", ";").option("header", "true")
      .load("src/main/resources/house.csv")

    import spark.implicits._
    // 开始shuffle
    // 打乱顺序
    val rand = new Random()
    val data = file.select("square", "price").map(row => {
      (row.getAs[String](0).toDouble, row.getString(1).toDouble, rand.nextDouble())
    }).toDF("square", "price", "rand").sort("rand")  // 强制类型转换过程

    val assembler = new VectorAssembler().setInputCols(Array("square")).setOutputCol("features")

    // 特征包装
    val dataset = assembler.transform(data)

    // 训练集，测试集
    // 拆分成训练数据集和测试数据集
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2))

    val lr = new LinearRegression().setLabelCol("price").setFeaturesCol("features")
      .setRegParam(0.3).setElasticNetParam(0.8).setMaxIter(10)
    val model = lr.fit(train)

    model.transform(test).show()
    val s = model.summary.totalIterations
    println(s"iter: ${s}")
  }
}
