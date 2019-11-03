package cn.edu.nju

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.sql.SparkSession

/**
 * Created by thpffcj on 2019/10/31.
 */
object MovieRecommendation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MovieRecommendation")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val parseRating = (string: String ) => {
      // 分割
      val stringArray = string.split("\t")
      // 包装
      Rating(stringArray(0).toInt, stringArray(1).toInt, stringArray(2).toFloat)
    }

    import spark.implicits._
    val data = spark.read.textFile("src/main/resources/u.data")
      // 分割
      .map(parseRating)
      // 转换成DataFrame
      .toDF("userId", "itemId", "rating")

//    data.show()

    val Array(train, test) = data.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(20)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")
      // 正则化参数
      .setRegParam(0.01)

    val model = als.fit(train)

    // 冷启动策略
    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)
    // 根据(userID,itemID)预测rating
//     predictions.show(false)

    // MovieLens数据集(学术界可靠的一种数据集) 给196号用户推荐10部电影
    val users = spark.createDataset(Array(196)).toDF("userID")
//    users.show(false)
    model.recommendForUserSubset(users, 10).show(false)

    // 模型评估
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error is $rmse \n")

//     Spark机器学习模型的持久化
//     模型保存
//     model.save("./xxx")
//     模型加载
//     val model = ALS.load("xxxx")
  }
}
