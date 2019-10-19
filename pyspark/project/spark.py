# -*- coding: UTF-8 -*-
# Created by thpffcj on 2019/10/19.
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("project").getOrCreate()

    df = spark.read.format("csv").load("file:///Users/thpffcj/Public/file/Beijing_2017_HourlyPM25_created20170803.csv")
    df.show

    spark.stop
