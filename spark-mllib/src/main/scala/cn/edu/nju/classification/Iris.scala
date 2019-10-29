package cn.edu.nju.classification

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * Created by thpffcj on 2019/10/29.
 */
object Iris {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Iris")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

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

    val assembler = new VectorAssembler().setInputCols(Array("_c0", "_c1", "_c2", "_c3")).setOutputCol("features")

    val dataset = assembler.transform(data)
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2))

    val dt = new DecisionTreeClassifier().setFeaturesCol("features").setLabelCol("label")
    val model = dt.fit(train)

    val result = model.transform(test)
    result.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(result)
    println(s"""accuracy is $accuracy""")
  }
}
