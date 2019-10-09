package cn.edu.nju.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Created by Thpffcj on 2018/5/7.
  * TopN统计Spark作业
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("D:/clean")

    //    accessDF.printSchema()
    //    accessDF.show(false)

    val day = "20180507"

    // 最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day)

    spark.stop()
  }

  /**
    * 按照地市进行统计TopN课程
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {

    /**
      * 使用DataFrame的方式进行统计
      */
    import spark.implicits._

    val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    videoAccessTopNDF.show(false)

    /**
      * 使用SQL的方式进行统计
      */
//    accessDF.createOrReplaceTempView("access_logs")
//    val videoAccessTopNDF = spark.sql("select day,cmsId, count(1) as times from access_logs " +
//      "where day='20180507' and cmsType='video' " +
//      "group by day,cmsId order by times desc")
//
//    videoAccessTopNDF.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          /**
            * 不建议大家在此处进行数据库的数据插入
            */
          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
