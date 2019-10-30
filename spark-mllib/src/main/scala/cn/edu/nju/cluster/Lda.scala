package cn.edu.nju.cluster

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * Created by thpffcj on 2019/10/29.
 */
object Lda {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("LDA")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 加载数据
    val file = spark.read.format("csv").load("src/main/resources/iris.data")

    val random = new Random()

    import spark.implicits._
    val data = file.map(row => {
      val label = row.getString(4) match {
        case "Iris-setosa" => 0
        case "Iris-versicolor" => 1
        case "Iris-virginica" => 2
      }

      (row.getString(0).toDouble, row.getString(1).toDouble,
        row.getString(2).toDouble, row.getString(3).toDouble,
        label, random.nextDouble())
    }).toDF("_c0", "_c1", "_c2", "_c3", "label", "rand").sort("rand")

    val assembler = new VectorAssembler()
      .setInputCols(Array("_c0", "_c1", "_c2", "_c3")).setOutputCol("features")

    val dataset = assembler.transform(data)
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2))

    // 训练一个LDA模型
    val lda = new LDA().setFeaturesCol("features").setK(3).setMaxIter(40)
    val model = lda.fit(train)

    // 展示结果
    val prediction = model.transform(test)
    prediction.show()

    val ll = model.logLikelihood(train)
    val lp = model.logPerplexity(train)

    // Describe topics.
    val topics = model.describeTopics(3)
    prediction.select("label", "topicDistribution").show(false)
    println("The topics described by their top-weighted terms:")
    topics.show(false)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")
  }
}
