package cn.edu.nju.emotionAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * Created by thpffcj on 2019/10/30.
 */
object EmotionAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("EmotionAnalysis")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 日志级别
    spark.sparkContext.setLogLevel("WARN")

    val rand = new Random()

    import spark.implicits._
    // 数据预处理
    val neg = spark.read.textFile("src/main/resources/neg.txt").map(line => {
      // 分词
      (line.split(" ").filter(!_.equals(" ")), 0, rand.nextDouble())
    }).toDF("words", "value", "random")

    val pos = spark.read.textFile("src/main/resources/pos.txt").map(line => {
      (line.split(" ").filter(!_.equals(" ")), 1, rand.nextDouble())
    }).toDF("words", "value", "random")  // 思考：这里把inner function提出重用来如何操作

    // 合并乱序
    val data = neg.union(pos).sort("random")
    data.show(false)
    println(neg.count(), pos.count(), data.count()) // 合并

    // 文本特征抽取(TF-IDF)
    val hashingTf = new HashingTF()
      .setInputCol("words")
      .setOutputCol("hashing")
      .transform(data)

    val idfModel = new IDF()
      .setInputCol("hashing")
      .setOutputCol("tfidf")
      .fit(hashingTf)

    val transformedData = idfModel.transform(hashingTf)
    val Array(training, test) = transformedData
      .randomSplit(Array(0.7, 0.3))

    // 根据抽取到的文本特征，使用分类器进行分类，这是一个二分类问题
    // 分类器是可替换的
    val bayes = new NaiveBayes()
      .setFeaturesCol("tfidf")  // X
      .setLabelCol("value")  // y 0:消极,1:积极
      .fit(training)

    // 交叉验证
    val result = bayes.transform(test)
    result.show(false)

    // 评估模型的准确率
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("value")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(result)
    println(s"""accuracy is $accuracy""")

    // 重构思考：
    // 尝试用pipeline重构代码
    // 尝试用模型预测随便属于一句话的情感，例如：
    // You are a bad girl,I hate you. ^_^
  }
}
